# Setup telemetry
from opentelemetry import propagate, trace
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

provider = TracerProvider(
    resource=Resource.create(attributes={SERVICE_NAME: "cwms-python-tests"})
)
provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
trace.set_tracer_provider(provider)
LoggingInstrumentor().instrument(inject_trace_context=True)
propagate.set_global_textmap(TraceContextTextMapPropagator())
