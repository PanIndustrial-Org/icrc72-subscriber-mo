# ICRC72 Subscriber Component

**This is alpha software. Use at your own risk**

The `icrc72-subscriber` module is a core component for subscribing to, managing, and processing events in an ICRC72-compliant ecosystem. It is designed to integrate seamlessly with orchestrators, broadcasters, and other related services, providing a robust and extensible foundation for event-driven architectures.

---

## Features

- **Subscription Management**: Easily register, update, and manage subscriptions.
- **Event Notification Handling**: Process notifications from broadcasters with built-in support for synchronization and retries.
- **Broadcaster Validation**: Ensures that only authorized broadcasters can send notifications.
- **Error Handling**: Customizable error handling for notification and event processing.
- **Backlog Management**: Handles out-of-order or delayed event notifications through a persistent backlog.
- **Cycle Sharing**: Implements ICRC85-compliant cycle-sharing to maintain efficient resource utilization.
- **Integration with Orchestrators**: Supports operations such as subscription updates and validation of broadcasters.

---

## Installation

Ensure your project includes dependencies like `mo:base`, `mo:star`, `mo:stableheapbtreemap`, and other libraries used in this module.

```bash
dfx deploy icrc72-subscriber
```

---

## Usage

### Initialization

The subscriber can be initialized using the `Init` function. It requires configuration parameters such as the orchestrator principal and optional environment-specific handlers.

```motoko
let subscriber = Init({
  manager: myManager;
  initialState: initialState();
  args: ?initArgs;
  pullEnvironment: ?environmentLoader;
  onInitialize: ?(subscriber -> async* ());
  onStorageChange: (state -> ());
});
```

### Registering Subscriptions

Register one or more subscriptions using `registerSubscriptions`. Each subscription can have a unique namespace and configuration.

```motoko
let results = await subscriber.registerSubscriptions([
  {
    namespace = "example.namespace";
    config = [];
    memo = null;
  }
]);
```

### Event Notification Handling

The `icrc72_handle_notification` method processes incoming notifications. It can be customized with synchronous or asynchronous handlers.

```motoko
subscriber.registerExecutionListenerAsync(?namespace, asyncHandler);
subscriber.registerExecutionListenerSync(?namespace, syncHandler);
```

### Subscription Updates

Update existing subscriptions via `updateSubscription`.

```motoko
let updates = [
  {
    subscription = #id(12345); 
    newConfig = [...];
  }
];
let updateResults = await subscriber.updateSubscription(updates);
```

### Statistics

Retrieve detailed statistics about the current state of the subscriber:

```motoko
let stats = subscriber.stats();
```

---

## API Reference

### Key Types

- `State`: Represents the internal state of the subscriber.
- `SubscriptionRegistration`: Defines the configuration for a new subscription.
- `EventNotification`: Represents a notification sent to the subscriber.
- `ExecutionItem`: Defines a handler for notifications (synchronous or asynchronous).

### Environment

The subscriber requires an environment configuration with handlers for:

- Adding records (`addRecord`)
- Handling event order (`handleEventOrder`)
- Calculating notification processing costs (`handleNotificationPrice`)
- Responding to subscription readiness (`onSubscriptionReady`)

### Constants

Predefined constants for namespaces, timers, and broadcaster actions are accessible via `CONST`.

---

## Advanced Topics

### Broadcaster Validation

The `validateBroadcaster` method ensures that only trusted broadcasters are authorized. It uses either a predefined list or ICRC75-compliant validation.

```motoko
let isValid = await subscriber.validateBroadcaster(caller);
```

### Cycle Sharing (ICRC85)

The module includes ICRC85-compliant cycle-sharing, which ensures efficient resource utilization based on the number of active actions.

---

## Debugging and Logging

Logs can be accessed through the `vecLog` property. Enable or disable specific debug channels via `debug_channel`.

---

## Contributing

Contributions are welcome! If you'd like to improve this component, please open a pull request or issue.

---

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## OVS Default Behavior

This motoko class has a default OVS behavior that sends cycles to the developer to provide funding for maintenance and continued development. In accordance with the OVS specification and ICRC85, this behavior may be overridden by another OVS sharing heuristic or turned off. We encourage all users to implement some form of OVS sharing as it helps us provide quality software and support to the community.

Default behavior: 1 XDR per 10000 processed events processed per month up to 100 XDR;

Default Beneficiary: PanIndustrial.com

Dependent Libraries: 
 - https://mops.one/timer-tool