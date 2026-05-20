import logging
import os
import sys

from openfeature import api
from openfeature.evaluation_context import EvaluationContext

from dynatrace.openfeature import DynatraceProvider

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y/%m/%d %H:%M:%S",
    stream=sys.stdout,
)

SDK_KEY = os.environ.get("DT_SDK_KEY", "")
CDN_BASE_URL = os.environ.get("DT_CDN_BASE_URL", None)

if not SDK_KEY:
    print("Set DT_SDK_KEY to your Dynatrace server SDK key and re-run.")
    sys.exit(1)

api.set_provider(DynatraceProvider(SDK_KEY, cdn_base_url=CDN_BASE_URL))
print("provider READY")

client = api.get_client()


def eval_flags(ctx: EvaluationContext) -> None:
    b = client.get_boolean_details("boolean-flag", False, ctx)
    s = client.get_string_details("string-flag", "", ctx)
    n = client.get_float_details("number-flag", 0, ctx)
    bv = f'variant="{b.variant or ""}"'
    sv = f'variant="{s.variant or ""}"'
    nv = f'variant="{n.variant or ""}"'
    print(f'  boolean-flag = {str(b.value).lower():<5}  {bv:<18}  reason={b.reason}')
    print(f'  string-flag  = {str(s.value)!r:<10}  {sv:<18}  reason={s.reason}')
    print(f'  number-flag  = {int(n.value):<4}   {nv:<18}  reason={n.reason}')


print()
print("--- without @dynatrace.com email (expect FALLBACK/default) ---")
eval_flags(EvaluationContext(targeting_key="user-456", attributes={"email": "test@external.com"}))

print()
print("--- with email=jonathan.norris@dynatrace.com (expect enabled) ---")
eval_flags(EvaluationContext(targeting_key="user-123", attributes={"email": "jonathan.norris@dynatrace.com"}))
print()

api.shutdown()
