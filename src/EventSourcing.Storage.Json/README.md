# EventSourcing.Storage.Json

Optional local durable adapter for atomic event batches and operation journal receipts.
Core remains filesystem independent. One exclusively leased store instance/process
owns a configured local path. See the repository transactional idempotency contract.
