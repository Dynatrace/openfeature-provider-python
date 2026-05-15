import os
import sys

from openfeature import api
from openfeature.evaluation_context import EvaluationContext

from dynatrace.openfeature import DynatraceProvider, DynatraceProviderOptions

SDK_KEY = os.environ.get('DT_SDK_KEY', '')

if not SDK_KEY:
    print('Set DT_SDK_KEY to your Dynatrace server SDK key and re-run.')
    sys.exit(1)

options = DynatraceProviderOptions.from_sdk_key(SDK_KEY)
print(f'Initializing provider (key={options.public_key})')

api.set_provider(DynatraceProvider(options))
client = api.get_client()

ctx = EvaluationContext(targeting_key='user-123')

value = client.get_boolean_value('my-bool-flag', False, ctx)
print(f'my-bool-flag = {value}')

str_value = client.get_string_value('my-string-flag', 'default', ctx)
print(f'my-string-flag = {str_value!r}')

api.shutdown()
print('Provider shut down.')
