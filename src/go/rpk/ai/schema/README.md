The canonical proto definitions live at:

  proto/redpanda/core/admin/v2/diagnostics.proto

That file defines the DiagnosticsService (GetEventLog, GetDiagnostics) and all
event types. `rpk debug event-log` and `rpk debug diagnostics` use the
generated ConnectRPC client to talk to this service.
