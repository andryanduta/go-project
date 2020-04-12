package handlerutil

import (
	"log"
)

// Handler provides mechanism to start any handler type
// Handler can be any transport type: HTTP, NSQ Consumer, Cron, etc
// Any handler should implement this interface so it can be passed to handler registrar
type Handler interface {
	// GetIdentity return handler's identity
	GetIdentity() string

	// Start initiates the handler so it listens for incoming requests
	Start() error
}

// Handlers keeps all added handlers
var handlers []Handler

// Add adds new handler to handlers
func Add(handler Handler) {
	handlers = append(handlers, handler)
}

// Start starts all added handlers
// It will panic if any handler fails to start
func Start() {
	var err error
	for _, h := range handlers {
		err = h.Start()
		if err != nil {
			log.Fatalf("[HANDLER REGISTRAR] error starting handler %s: %+v\n", h.GetIdentity(), err)
		}
	}
}
