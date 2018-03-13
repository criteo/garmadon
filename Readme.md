To debug instrumentation by ByteBuddy add this line in your agent builder 
```java
new AgentBuilder
  .Default()
  .with(AgentBuilder.Listener.StreamWriting.toSystemOut())
  ...
```

The output is sometime to verbose. You can use for the purpose the filtering listener
```java
.with(new AgentBuilder.Listener.Filtering(s -> s.contains("ContainersMonitorImpl"), AgentBuilder.Listener.StreamWriting.toSystemOut()))
```
