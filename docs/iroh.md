# Iroh

Go workers use the language-neutral bridge. Raw Arrow-mux:

```go
worker.RunIrohTcpUpstream("127.0.0.1", 9400, vgi.IrohBridgeOptions{
    Issuer: "production",
    TrustedProxyAddresses: []string{"127.0.0.1"},
    Authenticate: true,
})
```

For HTTP-over-Iroh, call `SetIrohBridge` with the same options before
`RunHttp`. The worker retains all HTTP limits, continuations, and externalized
batch behavior. The shared worker CLI accepts `--iroh-raw-upstream`,
`--iroh-issuer`, `--iroh-trusted-proxy`, and `--iroh-observe`.

The lower-level Go RPC client exposes an Iroh provider seam rather than
shipping a Go-native Iroh implementation. Applications may inject a provider;
otherwise use the packaged native clients or an ordinary local bridge.

