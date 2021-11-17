
| Type | Description |
| -    | -           |
| Document: | `Array | Object | ObjectValue` |
| Array: | `Comment? [Value? (, Value)*]` |
| Object: | `Comment? { ObjectValue }` |
| ObjectValue: | `(Key: Value)? (,|\n Key: Value)*` |
| Value: | `Object | Array | String | Number | Boolean` |
| Key: | `Comment? [a-zA-Z0-9-_]+ | String` |
| String: | `Comment? ('.*' | ".*")` |
| Number: | `Comment? -?\d*.\d*(e-?\d+)` |
| Boolean: | `Comment? true | false` |
| Comment: | `//.*\n | /* .* * /` |