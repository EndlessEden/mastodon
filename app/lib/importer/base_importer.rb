# frozen_string_literal: true

class Importer::BaseImporter
  # @param [Integer] batch_size
  # @param [Concurrent::ThreadPoolExecutor] executor
  def initialize(batch_size:, executor:)
    @batch_size = batch_size
    @executor   = executor
    @work_units = []
  end

  # Callback to run when a concurrent work unit completes
  # @param [Proc]
  def on_progress(&block)
    @on_progress = block
  end

  # Callback to run when a concurrent work unit fails
  # @param [Proc]
  def on_failure(&block)
    @on_failure = block
  end

  # Reduce resource usage during and improve speed of indexing
  def optimize_for_import!
    Chewy.client.indices.put_settings index: index.index_name, body: { index: { refresh_interval: -1 } }
  end

  # Restore original index settings
  def optimize_for_search!
    Chewy.client.indices.put_settings index: index.index_name, body: { index: { refresh_interval: index.settings_hash[:settings][:index][:refresh_interval] } }
  end

  # Estimate the amount of documents that would be indexed. Not exact!
  # @returns [Integer]
  def estimate!
    ActiveRecord::Base.connection_pool.with_connection { |connection| connection.select_one("SELECT reltuples AS estimate FROM pg_class WHERE relname = '#{index.adapter.target.table_name}'")['estimate'].to_i }
  end

  # Import data from the database into the index
  # @returns [Array<Integer>]
  def import!
    raise NotImplementedError
  end

  # Remove documents from the index that no longer exist in the database
  # @returns [Array<Integer>]
  def clean_up!
    @work_units = []

    index.scroll_batches do |documents|
      ids = documents.map { |doc| doc['_id'] }
      existence_map = index.adapter.default_scope.where(id: ids).pluck(:id).each_with_object({}) { |id, map| map[id.to_s] = true }

      in_work_unit(ids.reject { |id| existence_map[id] }) do |deleted_ids|
        bulk = Chewy::Index::Import::BulkBuilder.new(index, delete: deleted_ids).bulk_body

        Chewy::Index::Import::BulkRequest.new(index).perform(bulk)

        [0, deleted_ids.size]
      end
    end

    wait!
  end

  protected

  def in_work_unit(*args, &block)
    @work_units << Concurrent::Promises.future_on(@executor, *args, &block).on_fulfillment!(&@on_progress).on_rejection!(&@on_failure)
  rescue Concurrent::RejectedExecutionError
    sleep(0.1) && retry # Backpressure
  end

  def wait!
    Concurrent::Promises.zip(*@work_units).then { |*values| values.reduce([0, 0]) { |sum, value| [sum[0] + value[0], sum[1] + value[1]] } }.value!
  end

  def index
    raise NotImplementedError
  end
end
