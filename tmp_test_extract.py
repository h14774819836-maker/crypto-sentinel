import sys
import json
sys.path.append('.')
from app.ai.analyst import _extract_think_and_json

test_str_1 = '''<think>
this is some
thinking process
</think>
```json
{
  "market_regime": "uncertain",
  "signal": {
    "direction": "LONG"
  }
}
```'''

test_str_2 = '''{
  "market_regime": "uncertain",
  "signal": {
    "direction": "HOLD"
  }
}'''

test_str_3 = '''<think>just thinking</think>
{
  "market_regime": "uncertain"
}
'''

for i, test_str in enumerate([test_str_1, test_str_2, test_str_3]):
    print(f"--- Test {i+1} ---")
    think, json_str = _extract_think_and_json(test_str)
    print(f"Think: {think}")
    if json_str:
        print(f"JSON: {json.loads(json_str)}")
    else:
        print("JSON: None")
