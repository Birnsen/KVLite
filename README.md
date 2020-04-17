# What is KVLite
Basically it is a Key-Value Store based on SQLite. More specifically it uses 128 SQLite Databases which are addressed by the hashed key. For the hashing the [FNV Hash function](https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function) is used.
KVLite has also special functions to interact with Json directly in the SQLite Database based on the [SQLite Json1 Extension](https://sqlite.org/json1.html).

# Basic Usage
```C#
var kvl = KVLite.CreateInDirectory(new DirectoryInfo("/my/path"));
var key = Encoding.UTF8.GetBytes("key");
var value = Encoding.UTF8.GetBytes("value");
var newValue = Encoding.UTF8.GetBytes("newValue");

await kvl.Add(key, value);
var res = await kvl.Get(key);
await kvl.Update(key, newValue);
await kvl.Delete(key);

```
# Basic Json Usage
```C#
var kvl = KVLite.CreateJsonInDirectory(new DirectoryInfo("/my/path"));
var key = Encoding.UTF8.GetBytes("key");
var value = JsonSerializer.Serialize(new {hello = "value"});
var newValue = JsonSerializer.Serialize(new {hello = "newValue"});

await kvl.Add(key, value);
var res = await kvl.Get(key);
await kvl.Update(key, newValue);
await kvl.Delete(key);
```
# Json Extension Usage
```C#
var kvl = KVLite.CreateJsonInDirectory(new DirectoryInfo("/my/path"));
var key = Encoding.UTF8.GetBytes("key");
var value = JsonSerializer.Serialize(new {hello = "value"});

await kvl.Add(key, value);// {"hello": "value"}
await kvl.Insert(key, "$.world", "value"); // {"hello": "value", "world": "value"}
await kvl.Replace(key, "$.hello", "newValue");// {"hello": "newValue", "world": "value"}
await kvl.Set(key, "$.hello", "[1,2,3]");// {"hello": [1,2,3], "world": "value"}
await kvl.Insert(key, "$.hello[#]", 4); // {"hello": [1,2,3,4], "world": "value"}
```
# Who Uses KVLite
I do :)
# Why
Because!

