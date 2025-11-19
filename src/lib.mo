import MigrationTypes "migrations/types";
import MigrationLib "migrations";
import BTree "mo:stableheapbtreemap/BTree";
import OrchestrationService "./orchestratorService";
import BroadcasterService "./broadcasterService";
import Star "mo:star/star";
import ICRC77Service "../../icrc72-orchestrator.mo/src/ICRC77Service";

import Buffer "mo:base/Buffer";
import Error "mo:base/Error";
import Cycles "mo:base/ExperimentalCycles";

import Blob "mo:base/Blob";
import Int "mo:base/Int";
import Iter "mo:base/Iter";
import Nat "mo:base/Nat";
import Principal "mo:base/Principal";
import Text "mo:base/Text";
import Time "mo:base/Time";
import Service "service";

import D "mo:base/Debug";
import Array "mo:base/Array";
//import ClassPlusLib "../../../../ICDevs/projects/ClassPlus/src/";
import ClassPlusLib "mo:class-plus";
import Conversion = "mo:candy/conversion";
import Candy = "mo:candy/types";

import ovsfixed "mo:ovs-fixed";
import Timer "mo:base/Timer";

module {

  public let Migration = MigrationLib;

  public type State = MigrationTypes.State;

  public type CurrentState = MigrationTypes.Current.State;

  public type Environment = MigrationTypes.Current.Environment;
  public type NewEvent = MigrationTypes.Current.NewEvent;
  public type EmitableEvent = MigrationTypes.Current.EmitableEvent;
  public type Event = MigrationTypes.Current.Event;
  public type PublicationRegistration = MigrationTypes.Current.PublicationRegistration;
  public type SubscriptionRegistration = MigrationTypes.Current.SubscriptionRegistration;
  public type ExecutionHandler = MigrationTypes.Current.ExecutionHandler;
  public type ExecutionAsyncHandler = MigrationTypes.Current.ExecutionAsyncHandler;
  public type ExecutionItem = MigrationTypes.Current.ExecutionItem;
  public type ICRC16 = MigrationTypes.Current.ICRC16;
  public type ICRC16Map = MigrationTypes.Current.ICRC16Map;
  public type ICRC16Property = MigrationTypes.Current.ICRC16Property;
  public type ICRC16MapItem = MigrationTypes.Current.ICRC16MapItem;
  public type EventNotification = MigrationTypes.Current.EventNotification;
  public type InitArgs = MigrationTypes.Current.InitArgs;
  public type SubscriptionRecord = MigrationTypes.Current.SubscriptionRecord;
  public type Stats = MigrationTypes.Current.Stats;
  public type SubscriptionUpdateRequest = OrchestrationService.SubscriptionUpdateRequest;
  public type SubscriptionUpdateResult = OrchestrationService.SubscriptionUpdateResult;
  public type Value = MigrationTypes.Current.Value;

  // ICRC77 Types
  public type ReplayId = ICRC77Service.ReplayId;
  public type ReplayRegistration = ICRC77Service.ReplayRegistration;
  public type ReplayRegisterResult = ICRC77Service.ReplayRegisterResult;
  public type ReplayRegisterError = ICRC77Service.ReplayRegisterError;
  public type CancelReplayResult = ICRC77Service.CancelReplayResult;
  public type CancelReplayError = ICRC77Service.CancelReplayError;
  public type ReplayStatus = ICRC77Service.ReplayStatus;
  public type ReplayState = ICRC77Service.ReplayState;


  public let BTree = MigrationTypes.Current.BTree;
  public let Set = MigrationTypes.Current.Set;
  public let Vector = MigrationTypes.Current.Vector;
  public let Map = MigrationTypes.Current.Map;
  public let CONST = MigrationTypes.Current.CONST;
  public let TT = MigrationTypes.Current.TT;

  public let {phash} = Set;

  public let ONE_MINUTE = 60000000000 : Nat; //NanoSeconds
  public let FIVE_MINUTES = 300000000000 : Nat; //NanoSeconds
  public let ONE_SECOND = 1000000000 : Nat; //NanoSeconds
  public let THREE_SECONDS = 3000000000 : Nat; //NanoSeconds


  public let init = Migration.migrate;

  public func initialState() : State {#v0_0_0(#data)};
  public let currentStateVersion = #v0_1_0(#id);

  public func allowedSubscriberSelf(item: Principal) : (Text, ICRC16){
    return ("icrc72:subscription:subscribers:allowed:list", #Array([#Blob(Principal.toBlob(item))]));
  };

  /* public type ClassPlus = ClassPlusLib.ClassPlus<
    Subscriber, 
    State,
    InitArgs,
    Environment>;

  public func ClassPlusGetter(item: ?ClassPlus) : () -> Subscriber {
    ClassPlusLib.ClassPlusGetter<Subscriber, State, InitArgs, Environment>(item);
  }; */

  public func Init<system>(config : {
    manager: ClassPlusLib.ClassPlusInitializationManager;
    initialState: State;
    args : ?InitArgs;
    pullEnvironment : ?(() -> Environment);
    onInitialize: ?(Subscriber -> async*());
    onStorageChange : ((State) ->())
  }) :()-> Subscriber{

    D.print("Subscriber Init");
    switch(config.pullEnvironment){
      case(?_val) {
        D.print("pull environment has value");
        
      };
      case(null) {
        D.print("pull environment is null");
      };
    };  
    ClassPlusLib.ClassPlus<system,
      Subscriber, 
      State,
      InitArgs,
      Environment>({config with constructor = Subscriber}).get;
  };

  public func ReflectWithMaxStrategy(key: Text, max: Nat) : (<system>(state: CurrentState, environment: Environment, eventNotificication: EventNotification) -> Nat){
    let strategy = func <system>(_state: CurrentState, _environment: Environment, eventNotificication: EventNotification) : Nat {
      let useMax = max;
      let headers : ICRC16Map = switch(eventNotificication.headers){
        case(?val){val};
        case(null) return 0;
      };
      let config = Map.fromIter<Text, ICRC16>(headers.vals(), Map.thash);
      switch(Map.get<Text, ICRC16>(config, Map.thash, key)){
        case(?#Nat(val)) {
          if(val > useMax){
            return useMax;
          } else {
            return val;
          };
        };
        case(_){
          return 0;
        };
      };
    };
  };


  public class Subscriber(stored: ?State, _caller: Principal, canister: Principal, args: ?InitArgs, environment_passed: ?Environment, storageChanged: (State) -> ()){

    public let debug_channel = {
      var handleNotification = true;
      var startup = true;
      var announce = true;
    };

    public var vecLog = Vector.new<Text>();

    private func d(doLog : Bool, message: Text) {
      if(doLog){
        Vector.add(vecLog, Nat.toText(Int.abs(Time.now())) # " " # message);
        if(Vector.size(vecLog) > 5000){
          vecLog := Vector.new<Text>();
        };
        D.print(message);
      };
    };

    let environment = switch(environment_passed){
      case(?val) val;
      case(null) {
        D.trap("Environment is required");
      };
    };

    var state : CurrentState = switch(stored){
      case(null) {
        let #v0_1_0(#data(foundState)) = init(initialState(),currentStateVersion, null, canister);
        foundState;
      };
      case(?val) {
        let #v0_1_0(#data(foundState)) = init(val, currentStateVersion, null, canister);
        foundState;
      };
    };

    storageChanged(#v0_1_0(#data(state)));

    let self : Service.Service = actor(Principal.toText(canister));
    var defaultHandler : ?ExecutionItem = null;

    public var Orchestrator : OrchestrationService.Service = actor(Principal.toText(environment.icrc72OrchestratorCanister));
    
    public var ICRC77Orchestrator : ICRC77Service.Service = actor(Principal.toText(environment.icrc72OrchestratorCanister));

    private func natNow(): Nat{Int.abs(Time.now())};

    public func getState() : CurrentState {state};
    public func getEnvironment() : Environment {environment};

    private func fileSubscription(item: SubscriptionRecord) : () {
      ignore BTree.insert(state.subscriptionsByNamespace, Text.compare, item.namespace, item.id);
      ignore BTree.insert(state.subscriptions, Nat.compare, item.id, item);
    };

    /**
     * Check if an event notification is a replay event by examining headers.
     * Returns the replay ID if this is a replay event, null otherwise.
     */
    private func getReplayId(notification: EventNotification) : ?Nat {
      let ?headers = notification.headers else return null;
      let headerMap = Map.fromIter<Text, ICRC16>(headers.vals(), Map.thash);
      
      switch(Map.get(headerMap, Map.thash, "icrc77:replay")) {
        case(?#Bool(true)) {
          // This is a replay event, get the replay ID
          switch(Map.get(headerMap, Map.thash, "icrc77:replay:id")) {
            case(?#Nat(replayId)) ?replayId;
            case(_) null;
          };
        };
        case(_) null;
      };
    };

    /**
     * Check if a replay event indicates completion by examining the end_id header.
     */
    private func getReplayEndId(notification: EventNotification) : ?Nat {
      let ?headers = notification.headers else return null;
      let headerMap = Map.fromIter<Text, ICRC16>(headers.vals(), Map.thash);
      
      switch(Map.get(headerMap, Map.thash, "icrc77:replay:end_id")) {
        case(?#Nat(endId)) ?endId;
        case(_) null;
      };
    };

    /**
     * Process replay-specific logic for an event notification.
     * Updates replay status and handles completion detection.
     */
    private func handleReplayEvent<system>(notification: EventNotification, replayId: Nat) : () {
      debug d(debug_channel.handleNotification, "                    SUBSCRIBER: handling replay event " # debug_show(replayId, notification.eventId));
      
      // Update replay status with last event received
      let currentStatus = switch(BTree.get(state.replayStatus, Nat.compare, replayId)) {
        case(?status) status;
        case(null) (0, 0); // (lastEventIdSent, lastNotificationIdSent)
      };
      
      // Simple gap detection - check if we skipped event IDs
      if(currentStatus.0 > 0 and notification.eventId > currentStatus.0 + 1) {
        switch(environment.handleReplayGap) {
          case(?handler) handler<system>(state, environment, replayId, currentStatus.0 + 1, notification.eventId);
          case(null) {}; // Ignore gaps by default
        };
      };
      
      let updatedStatus = (notification.eventId, notification.notificationId);
      ignore BTree.insert(state.replayStatus, Nat.compare, replayId, updatedStatus);
      
      // Check if this notification indicates replay completion
      switch(getReplayEndId(notification)) {
        case(?endId) {
          debug d(debug_channel.handleNotification, "                    SUBSCRIBER: replay " # debug_show(replayId) # " completed at event " # debug_show(endId));
          // Replay is complete - could trigger a completion callback here
        };
        case(null) {
          debug d(debug_channel.handleNotification, "                    SUBSCRIBER: replay " # debug_show(replayId) # " continuing...");
        };
      };
    };

    //add new subscription
    public func registerSubscriptions(subscriptions: [SubscriptionRegistration]): async [OrchestrationService.SubscriptionRegisterResult] {
      debug d(debug_channel.announce, "                    SUBSCRIBER: registerSubscriptions " # debug_show(subscriptions));
      
      let result = try{
        await Orchestrator.icrc72_register_subscription(subscriptions);
      } catch(e){
        state.error := ?Error.message(e);
        return [];
      };
      var idx = 0;
      for(thisItem in result.vals()){
        switch(thisItem){
          case(?#Ok(val)) {

            fileSubscription({
              id = val;
              config = subscriptions[idx].config;
              namespace = subscriptions[idx].namespace;
            });
          };
          case(?#Err(val)) {
            debug d(debug_channel.announce, "                    SUBSCRIBER: registerSubscriptions error: " # debug_show(val));
            state.error := ?debug_show(val);
            //todo: should we retry?
          };
          case(null){
            debug d(debug_channel.announce, "                    SUBSCRIBER: registerSubscriptions error: null");
          }
        };
        idx += 1;
      };

      debug d(debug_channel.announce, "                    SUBSCRIBER: registerSubscriptions result: " # debug_show(result));
      result;
    };

    private let executionListeners = Map.new<Text, ExecutionItem>();

    public func registerExecutionListenerSync(namespace: ?Text, handler: ExecutionHandler) : () {
        
        debug d(debug_channel.announce, "                    SUBSCRIBER: registerExecutionListenerSync " # debug_show(namespace));
        let finalNamespace = switch(namespace){
          case(?val) val;
          case(null) "";
        };
        ignore Map.put<Text, ExecutionItem>(executionListeners, Map.thash, finalNamespace, #Sync(handler) : ExecutionItem);

        if(finalNamespace == ""){
          defaultHandler := ?(#Sync(handler): ExecutionItem);
        };
    };

    public func registerExecutionListenerAsync(namespace: ?Text, handler: ExecutionAsyncHandler) : () {
      let finalNamespace = switch(namespace){
        case(?val) val;
        case(null) "";
      };

      debug d(debug_channel.announce, "                    SUBSCRIBER: registerExecutionListenerAsync " # debug_show(finalNamespace));
      ignore Map.put<Text, ExecutionItem>(executionListeners, Map.thash, finalNamespace, #Async(handler) : ExecutionItem);

      if(finalNamespace == ""){
        defaultHandler := ?(#Async(handler): ExecutionItem)
      };
    };

    public func removeExecutionListener(namespace: Text) : () {
      ignore Map.remove<Text, ExecutionItem>(executionListeners, Map.thash, namespace);
    };

    private func secretWait() : async () {};


    public func icrc72_handle_notification<system>(caller: Principal, items : [EventNotification]) : async* () {

      //see https://m7sm4-2iaaa-aaaab-qabra-cai.raw.ic0.app/?tag=1234933381 for an example of why we handle things this way. This allows us to capture traps and provide notifications for errors for each individual trx.

      debug d(debug_channel.handleNotification, "                    SUBSCRIBER: icrc72_handle_notification " # debug_show(caller, canister) # " " # debug_show(items, caller));

      

      if(caller == canister and items.size()==1){

        debug d(debug_channel.handleNotification, "                    SUBSCRIBER: self call");

        //here we handle the self call with a single item
        let item = items[0];

        state.icrc85.activeActions += 1;

        debug d(debug_channel.handleNotification, "                    SUBSCRIBER: item " # debug_show(item));

        let _headerMap = switch(item.headers : ?ICRC16Map){
          case(null) Map.new<Text, ICRC16>();
          case(?val) Map.fromIter<Text, ICRC16>(val.vals(), Map.thash);
        };
      
        let namespace = item.namespace;

        let handler = if(namespace == ""){
          switch(defaultHandler){
            case(?val) val;
            case(null) {
              debug d(debug_channel.handleNotification, "                    SUBSCRIBER: no handler found: " # namespace);
              return;
            };
          }} else {
            switch(Map.get<Text, ExecutionItem>(executionListeners, Map.thash, namespace)){
            case(?val) val;
            case(null) {
              debug d(debug_channel.handleNotification, "                    SUBSCRIBER: no handler found: " # namespace);
              return;
            };
          };
        };

        debug d(debug_channel.handleNotification, "                    SUBSCRIBER: handler found: " # debug_show(namespace));

        //note: if a non-awaited trap occurs in one of the handlers, these items will not be replayed and they will need to be recovered and replayed. todo: would it make sense to call a self awaiting function here with a future so that they all get queued up with state change?  Would they execute in the same round?

        //note: there is a notification if there is a handler for awaited errors so that they can be handled. Sync error(traps) will be lost
        try{
        switch(handler){
          case(#Sync(val)) {
            val<system>(item);
            debug d(debug_channel.handleNotification, "                    SUBSCRIBER: handler done no trap sync: " # debug_show(namespace, item.headers));
          };
          case(#Async(val)) {
            await* val<system>(item);
            debug d(debug_channel.handleNotification, "                    SUBSCRIBER: handler done no trap async : " # debug_show(namespace, item.headers));
          };
        };
        } catch(e){
          debug d(debug_channel.handleNotification, "                    SUBSCRIBER: error in handler " # Error.message(e) # " " # debug_show(namespace, item));
          switch(environment.handleNotificationError){
            case(?val) val<system>(item, e);
            case(null) {};
          };
        };

        debug d(debug_channel.handleNotification, "                    SUBSCRIBER: handler done past trap: " # debug_show(namespace, item.headers));

        

        
        return;
      } else {

        

        //todo: check that the broadcaster is valid
        if((await* validateBroadcaster(caller)) == false){
          //todo: may need to add an event for notifying of illegal broadcaster and adding it to a block list
          debug d(debug_channel.handleNotification, "                    SUBSCRIBER: bad validate broadcaster " # debug_show(caller));
          return;
        };

        state.icrc85.activeActions := state.icrc85.activeActions + items.size();

        let subscriptionsHandled = Set.new<Nat>();

        label proc for(item in items.vals()){
          //we quickly hand these to our self with awaits to be able to trap errors

          debug d(debug_channel.handleNotification, "                    SUBSCRIBER: processing item " # debug_show((item, BTree.toArray(state.subscriptionsByNamespace))));

          //find the subscription
          let ?subscriptionId = BTree.get(state.subscriptionsByNamespace, Text.compare, item.namespace) else {
            debug d(debug_channel.handleNotification, "                    SUBSCRIBER: no subscription found for namespace: " # item.namespace);
            continue proc;
          };

          debug d(debug_channel.handleNotification, "                    SUBSCRIBER: subscription found for namespace: " # item.namespace # " " # debug_show(subscriptionId));

          // Check if this is a replay event and handle accordingly
          let isReplayEvent = switch(getReplayId(item)) {
            case(?replayId) {
              debug d(debug_channel.handleNotification, "                    SUBSCRIBER: processing replay event " # debug_show(replayId));
              
              // Simple out-of-band detection - check if we know about this replay
              switch(BTree.get(state.replays, Nat.compare, replayId)) {
                case(?_) {
                  // Known replay - process normally
                  handleReplayEvent<system>(item, replayId);
                };
                case(null) {
                  // Unknown replay - call handler if available
                  switch(environment.handleOutOfBandReplay) {
                    case(?handler) handler<system>(state, environment, item, "Unknown replay ID: " # debug_show(replayId));
                    case(null) {}; // Ignore by default
                  };
                };
              };
              true;
            };
            case(null) {
              // Regular event, no special replay handling needed
              false;
            };
          };

          // we add the notification to the block here as it is the first time we confirm that we've seen it and that we have a subscription for it.
          let _trxid = switch(environment.addRecord){
            case (?addRecord) {
              //todo: calculate value of blocks
              let txtop = Buffer.fromIter<(Text, Value)>([("btype",#Text("72Notification")),("ts", #Nat(natNow()))].vals());
              let tx = Buffer.fromIter<(Text, Value)>([
                ("namespace", #Text(item.namespace) : Value) : (Text,Value),
                ("notificationId", #Nat(item.notificationId): Value) : (Text,Value),
                ("eventId", #Nat(item.eventId): Value) : (Text,Value),
                
                ("timestamp", #Nat(item.timestamp): Value) : (Text,Value),
                ("publisher", #Blob(Principal.toBlob(item.source)): Value) : (Text,Value),
                
                
                
              ].vals());

              switch(item.headers){
                case(?val) {
                  if(val.size() > 0){
                    tx.add(("headers", Conversion.CandySharedToValue(#Map(val) : Candy.CandyShared): Value): (Text,Value));
                  };
                };
                case(null) {};
              };

              
              
              tx.add(("data", Conversion.CandySharedToValue(item.data : Candy.CandyShared): Value): (Text,Value));
            
              

              switch(item.prevEventId){
                case(?val) {
                  tx.add(("prevEventId", #Nat(val)));
                };
                case(null) {};
              };
              switch(item.filter){
                case (?filter) {
                  tx.add(("icrc72:subscription:filter", #Text(filter)));
                };
                case (null) {};
              };
              addRecord(Buffer.toArray(tx), ?Buffer.toArray(txtop));
            };
            case (null) 0;
          };


          //check the order of receipt if desired
          // Replay events bypass normal ordering constraints since they are historical
          let canProceed = if(isReplayEvent) {
            debug d(debug_channel.handleNotification, "                    SUBSCRIBER: bypassing order check for replay event " # debug_show(item.eventId));
            true;
          } else {
            switch(environment.handleEventOrder){
              case(?val) val<system>(state, environment, subscriptionId, item);
              case(null) true;
            };
          };

          if(canProceed == false){
            debug d(debug_channel.handleNotification, "                    SUBSCRIBER: cannot proceed for item " # debug_show(item));
            let backlog = switch(BTree.get(state.backlogs, Nat.compare, subscriptionId)){
              case(?val) val;
              case(null) {
                let newMap = BTree.init<Nat, EventNotification>(null);
                ignore BTree.insert(state.backlogs, Nat.compare, subscriptionId, newMap);
                newMap;
              };
            };
            ignore BTree.insert<Nat, EventNotification>(backlog, Nat.compare, item.notificationId, item);
            continue proc;
          };

          debug d(debug_channel.handleNotification, "                    SUBSCRIBER: can proceed for item " # debug_show(item));

          Set.add(subscriptionsHandled, Set.nhash, subscriptionId);

          //register that this was the last id handled by this notifier
          //todo: turn off for people that don't track
          let idCol = switch(BTree.get(state.lastEventId, Text.compare, item.namespace)){
            case(?val) val;
            case(null) {
              let newMap = BTree.init<Nat, Nat>(null);
              ignore BTree.insert(state.lastEventId, Text.compare, item.namespace, newMap);
              newMap
            };
          };

          ignore BTree.insert(idCol, Nat.compare, subscriptionId, item.eventId);

          let headerMap = switch(item.headers){
            case(null) Map.new<Text, ICRC16>();
            case(?val) Map.fromIter<Text, ICRC16>(val.vals(), Map.thash);
          };

          debug d(debug_channel.handleNotification, "                    SUBSCRIBER: headerMap " # debug_show(headerMap));

          debug d(debug_channel.handleNotification, "                    SUBSCRIBER: calling handle notification on self" # debug_show(item));

          //todo: may need a limiter here to prevent too many in the outgoing queue.
          self.icrc72_handle_notification([item]);

          //we go ahead and add the accumultor for confirmations here so that they are confirmed even if the item fails.
          let relayBlob = Map.get(headerMap, Map.thash, "icrc72:relay");

          let ?#Blob(broadcasterBlob) = Map.get(headerMap, Map.thash, "icrc72:broadcaster") else {
            debug d(debug_channel.handleNotification, "                    SUBSCRIBER: no broadcaster found" # debug_show(item.headers));
            continue proc;
          };

          debug d(debug_channel.handleNotification, "                    SUBSCRIBER: broadcaster found " # debug_show(Principal.fromBlob(broadcasterBlob)));

          let confirmPrincipal = switch(relayBlob){
            case(?val) {
              switch(val){
                case(#Blob(val)){
                  debug d(debug_channel.handleNotification, "                    SUBSCRIBER: relay found " # debug_show(Principal.fromBlob(val)));
                 Principal.fromBlob(val);
                };
                case(_) {
                  debug d(debug_channel.handleNotification, "                    SUBSCRIBER: invalid relay found" # debug_show(relayBlob));
                  continue proc;
                };
              };
            };
            case(null) Principal.fromBlob(broadcasterBlob);
          };

          debug d(debug_channel.handleNotification, "                    SUBSCRIBER: broadcaster or relay found " # debug_show(confirmPrincipal));


          let accumulator = switch(BTree.get(state.confirmAccumulator, Principal.compare, confirmPrincipal)){
            case(null){
              debug d(debug_channel.handleNotification, "                    SUBSCRIBER: no accumulator found " # debug_show(confirmPrincipal));

              let newVector = Vector.new<(Nat,Nat)>();
              ignore BTree.insert(state.confirmAccumulator, Principal.compare, confirmPrincipal, newVector);
              newVector;
            };
            case(?val) {val};
          };

          let cycles = switch(environment.handleNotificationPrice){
            case(?val) val<system>(state, environment, item);
            case(null) 0;
          };
          Vector.add(accumulator, (item.notificationId, cycles));

          debug d(debug_channel.handleNotification, "                    SUBSCRIBER: accumulator used " # debug_show(accumulator, item.notificationId));
          
        };

        debug d(debug_channel.handleNotification, "                    SUBSCRIBER: subscriptionsHandled before await" # debug_show(subscriptionsHandled));
        await secretWait();

        debug d(debug_channel.handleNotification, "                    SUBSCRIBER: subscriptionsHandled after await" # debug_show(subscriptionsHandled));

        //handle backlogs
        let backlogBuffer = Buffer.Buffer<EventNotification>(1);
        label procSub for(thisSub in Set.keys(subscriptionsHandled)){

          debug d(debug_channel.handleNotification, "                    SUBSCRIBER: processing backlog for " # debug_show(thisSub));

          let backlog = switch(BTree.get(state.backlogs, Nat.compare, thisSub)){
            case(?val) val;
            case(null) {
              continue procSub;
            };
          };
          var min = BTree.min(backlog);
          let testMin = switch(min){
            case(?val) val;
            case(null) {
              ignore BTree.delete(state.backlogs, Nat.compare, thisSub);
              continue procSub;
            };
          };
          // Check if the first item in backlog is a replay event
          let isBacklogReplayEvent = switch(getReplayId(testMin.1)) {
            case(?_) true;
            case(null) false;
          };
          
          var canProceed = if(isBacklogReplayEvent) {
            debug d(debug_channel.handleNotification, "                    SUBSCRIBER: bypassing order check for backlog replay event " # debug_show(testMin.1.eventId));
            true;
          } else {
            switch(environment.handleEventOrder){
              case(?val) val<system>(state, environment, thisSub, testMin.1);
              case(null) true;
            };
          };

          label continuous while (canProceed){
            debug d(debug_channel.handleNotification, "                    SUBSCRIBER: backlog in continuous" # debug_show(backlog));
            let thisItem = switch(min){
              case(?val) val;
              case(null) {
                break continuous;
              };
            };

            backlogBuffer.add(thisItem.1);
            if(BTree.size(backlog) < 2){
              ignore BTree.delete(state.backlogs, Nat.compare, thisSub);
              continue procSub;
            } else {
              ignore BTree.delete(backlog, Nat.compare, testMin.0);
            };
            
            // Check next item for replay event status before checking order
            min := BTree.min(backlog);
            let nextTestMin = switch(min){
              case(?val) val;
              case(null) {
                ignore BTree.delete(state.backlogs, Nat.compare, thisSub);
                continue procSub;
              };
            };
            
            let isNextReplayEvent = switch(getReplayId(nextTestMin.1)) {
              case(?_) true;
              case(null) false;
            };
            
            canProceed := if(isNextReplayEvent) {
              debug d(debug_channel.handleNotification, "                    SUBSCRIBER: bypassing order check for next backlog replay event " # debug_show(nextTestMin.1.eventId));
              true;
            } else {
              switch(environment.handleEventOrder){
                case(?val) val<system>(state, environment, thisSub, nextTestMin.1);
                case(null) true;
              };
            };
          };
        };

        debug d(debug_channel.handleNotification, "                    SUBSCRIBER: backlogBuffer " # debug_show(backlogBuffer.size()));

        if(backlogBuffer.size() > 0){
          debug d(debug_channel.handleNotification, "                    SUBSCRIBER: backlogBuffer " # debug_show(backlogBuffer.size()));
          self.icrc72_handle_notification(Buffer.toArray<EventNotification>(backlogBuffer));
        };
      };

      debug d(debug_channel.handleNotification, "                    SUBSCRIBER: icrc72_handle_notification done setting timer for drain");

      if(state.confirmTimer == null){
        state.confirmTimer := do{
          let id = environment.tt.setActionASync<system>(natNow(), {
                actionType = CONST.subscriber.timers.sendConfirmations;
                params = to_candid([]);
              }, FIVE_MINUTES);
          ?id.id;
        };
      };

    };

    private func drainConfirmations<system>(actionId: TT.ActionId, action: TT.Action) : async* Star.Star<TT.ActionId, TT.Error> {

      debug d(debug_channel.handleNotification, "                    SUBSCRIBER: drainConfirmations " # debug_show(actionId) # " " # debug_show(action));

      let proc = BTree.toArray(state.confirmAccumulator);
      BTree.clear(state.confirmAccumulator);
      state.confirmTimer := null;

      debug d(debug_channel.handleNotification, "                    SUBSCRIBER: confirmAccumulator " # debug_show(proc));

      for(thisItem in proc.vals()){

        debug d(debug_channel.handleNotification, "                    SUBSCRIBER: confirmAccumulator item " # debug_show((thisItem, thisItem.1)));
        let broadcaster : BroadcasterService.Service = actor(Principal.toText(thisItem.0));

        var cycles = 0;
        let list = Buffer.Buffer<Nat>(Vector.size(thisItem.1));

        for(thisEntry in Vector.vals(thisItem.1)){
          cycles += thisEntry.1;
          list.add(thisEntry.0);
        };

        if(cycles > 0){
          Cycles.add<system>(cycles);
        };

        //add clycles
        ignore await broadcaster.icrc72_confirm_notifications(Buffer.toArray(list));
      };
      return #awaited(actionId);
    };

    public func validateBroadcaster(caller: Principal) : async* Bool {
      //debug d(debug_channel.announce, "                    SUBSCRIBER: validateBroadcaster " # debug_show(caller) # " " # debug_show(state.validBroadcasters));
      switch(state.validBroadcasters){
        case(#list(val)) {
          return Set.has(val, phash, caller);
        };
        case(#icrc75(_val)) {
          //todo: implement icrc75
          return false;
        };
      };
    };

    public func fileBroadcaster(broadcaster: Principal, subscriptionId : Nat, namespace: Text){

      debug d(debug_channel.announce, "                    SUBSCRIBER: fileBroadcaster from subscriber" # debug_show(broadcaster) # " " # debug_show(subscriptionId) # " " # debug_show(namespace));

      let broadcasters = switch(BTree.get(state.broadcasters, Nat.compare, subscriptionId)){
          case(null) {
            let col = Vector.new<Principal>();
            ignore BTree.insert(state.broadcasters, Nat.compare, subscriptionId, col);
            col
          };
          case(?val) {val};
        };

        switch(Vector.indexOf<Principal>(broadcaster, broadcasters, Principal.equal)){
          case(?_val) {};
          case(null) {
            Vector.add(broadcasters, broadcaster);
          };
        };
    };


    //life cycle
    private func handleBroadcasterEvents<system>(notification: EventNotification) : async* (){

      debug d(debug_channel.handleNotification, "                    SUBSCRIBER: handleBroadcasterEvents " # debug_show(notification));

      let #Map(data) = notification.data else {
        return;
      };
      for(thisItem in data.vals()){
        if(thisItem.0 == CONST.broadcasters.subscriber.broadcasters.add){
          //can now register subscriptions
          //we could check the id here if we save it in state, but these should be the only thing we see here.
          //notification sources should only a valid broadcaster
          if((await* validateBroadcaster(notification.source)) == false){
            //todo: log something
            debug d(debug_channel.handleNotification, "                    SUBSCRIBER: invalid broadcaster");
            return;
          };

          debug d(debug_channel.handleNotification, "                    SUBSCRIBER: broadcaster validated");

          let #Array(newData) = thisItem.1 else return;

          for(thisAdd in newData.vals()){

            debug d(debug_channel.handleNotification, "                    SUBSCRIBER: broadcaster add " # debug_show(thisAdd));
            let #Array(itemData) = thisAdd else return;
            let #Text(subscriptionNamespace) = itemData[0] else return;
            let #Blob(principalBlob) = itemData[1] else return;
            let principal = Principal.fromBlob(principalBlob);

            let subscriptionId = switch(BTree.get(state.subscriptionsByNamespace, Text.compare, subscriptionNamespace)){
              case(?val) val;
              case(null) {
                debug d(debug_channel.handleNotification, "                    SUBSCRIBER: no subscription found for namespace: " # subscriptionNamespace);
                return;
              };
            };

            //todo: can be optimized
            let currentSize = do?{state.broadcasters |>
              BTree.get(_, Nat.compare, subscriptionId) |>
              Vector.size(_!)};

            fileBroadcaster(principal, subscriptionId, subscriptionNamespace);

             if(currentSize == null or currentSize == ?0){
              debug d(debug_channel.announce, "                    SUBSCRIBER: about to call subscription ready for" # subscriptionNamespace);
              switch(environment.onSubscriptionReady){
                case(?val){
                  val<system>(state, environment, subscriptionNamespace, subscriptionId);
                };
                case(null){};
              };
            } else {
              debug d(debug_channel.announce, "          SUBSCRIBER: Already has broadcasters");
            };
          };  
          
        } else if(data[0].0 == CONST.subscriber.broadcasters.remove){
          //todo: fix later
          /* debug d(debug_channel.handleNotification, "                    SUBSCRIBER: broadcaster remove");
          if(notification.source != environment.icrc72OrchestratorCanister){

          let #Array(newData) = data[0].1 else return;
          let #Nat(subscriptionId) = newData[0] else return;
          let #Blob(principalBlob) = newData[1] else return;
          let principal = Principal.fromBlob(principalBlob);

          let broadcasters : Vector.Vector<Principal> = switch(BTree.get(state.broadcasters, Nat.compare, subscriptionId)){
            case(null) { return;}; //nothing to do
            case(?val) {val};
          };

          switch(Vector.indexOf(principal, broadcasters, Principal.equal)){
            case(?val) {
              let newVector = Vector.new<Principal>();
              label remake for(thisItem in Vector.vals(broadcasters)){
                if(Principal.equal(thisItem, principal)){
                  continue remake;
                };
                Vector.add(newVector, thisItem);
              };
              ignore BTree.insert(state.broadcasters, Nat.compare, subscriptionId, newVector);
            };
            case(null) {
              return; //nothing to do
            };
          }; */
        } else if(data[0].0 == CONST.subscriber.broadcasters.error){
          state.error := ?debug_show(notification);
        } else if( data[0].0 == CONST.subscriber.replay.add){
          debug d(debug_channel.handleNotification, "                    SUBSCRIBER: replay add");
          
          let #Array(newData) = thisItem.1 else return;
          
          for(thisReplayAdd in newData.vals()){
            debug d(debug_channel.handleNotification, "                    SUBSCRIBER: replay add item " # debug_show(thisReplayAdd));
            let #Array(replayData) = thisReplayAdd else return;
            let #Nat(replayId) = replayData[0] else return;
            let #Blob(broadcasterBlob) = replayData[1] else return;
            let broadcasterPrincipal = Principal.fromBlob(broadcasterBlob);
            
            debug d(debug_channel.handleNotification, "                    SUBSCRIBER: updating replay " # debug_show(replayId) # " with broadcaster " # debug_show(broadcasterPrincipal));
            
            // Find the existing replay record and update it with the broadcaster
            let ?existingReplay = BTree.get(state.replays, Nat.compare, replayId) else {
              debug d(debug_channel.handleNotification, "                    SUBSCRIBER: replay not found " # debug_show(replayId));
              return;
            };
            
            // Update the replay record with the broadcaster principal (second field in tuple)
            let updatedReplay = (existingReplay.0, ?broadcasterPrincipal, existingReplay.2, existingReplay.3, existingReplay.4);
            ignore BTree.insert(state.replays, Nat.compare, replayId, updatedReplay);
            
            debug d(debug_channel.handleNotification, "                    SUBSCRIBER: replay updated successfully " # debug_show(replayId));
          };
        } else if( data[0].0 == CONST.subscriber.replay.remove){
          debug d(debug_channel.handleNotification, "                    SUBSCRIBER: replay remove");
          
          let #Array(newData) = thisItem.1 else return;
          
          for(thisReplayRemove in newData.vals()){
            debug d(debug_channel.handleNotification, "                    SUBSCRIBER: replay remove item " # debug_show(thisReplayRemove));
            let #Array(replayData) = thisReplayRemove else return;
            let #Nat(replayId) = replayData[0] else return;
            
            debug d(debug_channel.handleNotification, "                    SUBSCRIBER: removing replay " # debug_show(replayId));
            
            // Remove the replay record from state
            ignore BTree.delete(state.replays, Nat.compare, replayId);
            ignore BTree.delete(state.replayStatus, Nat.compare, replayId);
            
            debug d(debug_channel.handleNotification, "                    SUBSCRIBER: replay removed successfully " # debug_show(replayId));
          };
        } else {
          //unknown command
          debug d(debug_channel.handleNotification, "                    SUBSCRIBER: unknown command" # debug_show(data[0].0));
        };
      }; // Close the for(thisItem in data.vals()) loop
    }; // Close the handleBroadcasterEvents function

    public type SubscribeRequestItem = {
      namespace : Text;
      config : ICRC16Map;
      memo : ?Blob;
      listener : ExecutionItem;
    };

    public type SubscribeRequest = [SubscribeRequestItem];

    public func subscribe(request: SubscribeRequest) : async* [OrchestrationService.SubscriptionRegisterResult] {
      debug d(debug_channel.announce, "                    SUBSCRIBER: subscribe " # debug_show(request.size()));

      await* ensureCycleShare();

      for(thisItem in request.vals()){
        switch(thisItem.listener){
          case(#Async(val)) {
            registerExecutionListenerAsync(?thisItem.namespace, val);
          };
          case(#Sync(val)) {
            registerExecutionListenerSync(?thisItem.namespace, val);
          };
        };
      };

      let result = await registerSubscriptions(
        Array.map<SubscribeRequestItem, OrchestrationService.SubscriptionRegistration> (request, func(item: SubscribeRequestItem) : OrchestrationService.SubscriptionRegistration {
          {
            namespace = item.namespace;
            config = item.config;
            memo = item.memo;
          }
        })
      );

      debug d(debug_channel.announce, "                    SUBSCRIBER: subscribe result " # debug_show(result));
      result;
    };

    /**
     * Request a replay of events for specified namespaces and ranges.
     * This function registers replay requests with the ICRC77 orchestrator
     * and records them in the subscriber's state with unassigned broadcasters.
     *
     * @param request - Array of replay registrations specifying namespace, range, config, etc.
     * @returns Array of replay registration results containing replay IDs or errors
     */
    public func requestReplay(request: [ReplayRegistration]) : async [ReplayRegisterResult] {
      debug d(debug_channel.announce, "                    SUBSCRIBER: requestReplay " # debug_show(request.size()));

      await* ensureCycleShare();

      // Validate that the subscriber has subscriptions for the requested namespaces
      for(thisItem in request.vals()) {
        switch(BTree.get(state.subscriptionsByNamespace, Text.compare, thisItem.namespace)) {
          case(null) {
            debug d(debug_channel.announce, "                    SUBSCRIBER: No subscription found for namespace: " # thisItem.namespace);
            // We could return an error here, but let the orchestrator handle validation
          };
          case(?subscriptionId) {
            debug d(debug_channel.announce, "                    SUBSCRIBER: Found subscription " # debug_show(subscriptionId) # " for namespace: " # thisItem.namespace);
          };
        };
      };

      let result = try {
        await ICRC77Orchestrator.icrc77_register_replay(request);
      } catch(e) {
        state.error := ?Error.message(e);
        debug d(debug_channel.announce, "                    SUBSCRIBER: Error requesting replay: " # Error.message(e));
        return [];
      };

      // Process results and update state for successful replay registrations
      var idx = 0;
      for(thisResult in result.vals()) {
        switch(thisResult) {
          case(?#Ok(replayId)) {
            let requestItem = request[idx];
            // Record replay in state with unassigned broadcaster (null)
            // Structure: (namespace, broadcaster, filter, skip, range)
            let filter = switch(Map.get(Map.fromIter<Text, ICRC16>(requestItem.config.vals(), Map.thash), Map.thash, "icrc72:subscription:filter")) {
              case(?#Text(filterText)) ?filterText;
              case(_) null;
            };
            
            let skip = switch(Map.get(Map.fromIter<Text, ICRC16>(requestItem.config.vals(), Map.thash), Map.thash, "icrc72:subscription:skip")) {
              case(?#Array(skipArray)) {
                if (skipArray.size() >= 2) {
                  switch(skipArray[0], skipArray[1]) {
                    case(#Nat(seed), #Nat(offset)) ?(seed, offset);
                    case(_, _) null;
                  };
                } else null;
              };
              case(_) null;
            };

            ignore BTree.insert(state.replays, Nat.compare, replayId, (
              requestItem.namespace,
              null : ?Principal, // Broadcaster will be assigned later via system messages
              filter,
              skip,
              requestItem.range
            ));

            debug d(debug_channel.announce, "                    SUBSCRIBER: Recorded replay " # debug_show(replayId) # " for namespace: " # requestItem.namespace);
          };
          case(?#Err(error)) {
            debug d(debug_channel.announce, "                    SUBSCRIBER: Replay request failed: " # debug_show(error));
          };
          case(null) {
            debug d(debug_channel.announce, "                    SUBSCRIBER: Null result for replay request");
          };
        };
        idx += 1;
      };

      debug d(debug_channel.announce, "                    SUBSCRIBER: requestReplay result " # debug_show(result));
      result;
    };

    var _isInit = false;

    //register subscription for the system events
    public func initializeSubscriptions() : async() {
      
      if(_isInit == true) return;
      _isInit := true;
      debug d(debug_channel.startup, "                    SUBSCRIBER: initSubscriber");

      registerExecutionListenerAsync(?(CONST.subscriber.sys # Principal.toText(canister)), handleBroadcasterEvents);

      environment.tt.registerExecutionListenerAsync(?CONST.subscriber.timers.sendConfirmations, drainConfirmations); 

      let subscriptionResult = await registerSubscriptions([{
        namespace = CONST.subscriber.sys # Principal.toText(canister);
        config = [];
        memo = null
      }]);

      debug d(debug_channel.startup, "                    SUBSCRIBER: subscriptionResult " # debug_show(subscriptionResult));

      //todo: check for valid broadcasters
      let validBroadcasters = try{
        await Orchestrator.icrc72_get_valid_broadcaster();
      } catch(e){
        //how to dea with this?
        debug d(debug_channel.startup, "                    SUBSCRIBER: error getting valid broadcasters " # Error.message(e));
        state.error := ?Error.message(e);
        return;
      };

      debug d(debug_channel.startup, "                    SUBSCRIBER: valid broadcasters retrieved " # debug_show(validBroadcasters));

      switch(validBroadcasters){
        case(#list(val)) {
          if(val.size() > 0){
            state.readyForSubscription := true;
          } else{
            //set the timer to check again?
            state.error := ?"no valid broadcasters";
          };
          state.validBroadcasters := #list(Set.fromIter(val.vals(), phash));
        };
        case(#icrc75(val)) {
          state.validBroadcasters := #icrc75(val);
        };
      };
    };

    public func stats(): Stats {
      return {
        icrc72OrchestratorCanister = environment.icrc72OrchestratorCanister;
        broadcasters = Iter.toArray(Iter.map<(Nat, Vector.Vector<Principal>), (Nat, [Principal])>(BTree.entries(state.broadcasters), func(nat:Nat, vec: Vector.Vector<Principal>) { (nat, Vector.toArray(vec)) }));

        subscriptions = BTree.toArray(state.subscriptions);

        validBroadcasters = switch(state.validBroadcasters) {
          case (#list(set)) #list(Set.toArray(set));
          case (#icrc75(item)) #icrc75(item);
        };
        icrc85 = {
          nextCycleActionId = state.icrc85.nextCycleActionId;
          lastActionReported = state.icrc85.lastActionReported;
          activeActions = state.icrc85.activeActions;
        };

        confirmAccumulator = Iter.toArray(Iter.map<(Principal, Vector.Vector<(Nat, Nat)>), (Principal, [(Nat,Nat)])>(BTree.entries(state.confirmAccumulator), func(principal : Principal, vec: Vector.Vector<(Nat,Nat)>):(Principal, [(Nat,Nat)]) {(principal, Vector.toArray(vec))}));

        confirmTimer = state.confirmTimer;

        lastEventId = Iter.toArray(Iter.map<(Text, BTree.BTree<Nat, Nat>), (Text, [(Nat, Nat)])>(BTree.entries(state.lastEventId),func (namespace: Text, btree : BTree.BTree<Nat, Nat>):(Text, [(Nat, Nat)]){(namespace, BTree.toArray(btree))}));

        backlogs = Iter.toArray(Iter.map<(Nat, BTree.BTree<Nat, EventNotification>), (Nat, [(Nat, EventNotification)])>(BTree.entries(state.backlogs), func(id: Nat, btree: BTree.BTree<Nat, EventNotification>) : (Nat, [(Nat, EventNotification)]){ (id, BTree.toArray(btree))}));

        replays = BTree.toArray(state.replays);
        replayStatus = BTree.toArray(state.replayStatus);

        readyForSubscription = state.readyForSubscription;
        error = state.error;
        tt = environment.tt.getStats();
        log = Vector.toArray(vecLog);
      };
    };

    /**
     * Update existing subscriptions.
     *
     * @param updates - A list of SubscriptionUpdateRequest specifying which subscriptions to update and how.
     * @returns A list of SubscriptionUpdateResult indicating success or failure for each update.
     */
    public func updateSubscription(updates: [SubscriptionUpdateRequest]) : async* [SubscriptionUpdateResult] {
        // Logging the update request
        debug d(debug_channel.announce, "                    SUBSCRIBER: updateSubscription called with " # debug_show(updates.size()) # " updates");

        // Attempt to notify the Orchestrator about the updates
        let orchestratorResults: [SubscriptionUpdateResult] = try {
            await Orchestrator.icrc72_update_subscription(updates);
        } catch(err) {
            // Log the error and return failure for all updates
            debug d(debug_channel.announce, "                    SUBSCRIBER: Failed to notify Orchestrator: " # Error.message(err));
            state.error := ?("Failed to communicate with Orchestrator: " # Error.message(err));
            // Return a list of generic errors corresponding to each update
            return Array.map<SubscriptionUpdateRequest, SubscriptionUpdateResult>(updates, func(_) : SubscriptionUpdateResult {
                ?#Err(#GenericError { error_code = 0; message = "Orchestrator communication failure" });
            });
        };

        var results = Vector.new<SubscriptionUpdateResult>();
        let updateCount = orchestratorResults.size();

        let subsUpdated = Set.new<Nat>();

        debug d(debug_channel.announce, "                    SUBSCRIBER: updateSubscription received " # debug_show(updateCount) # " results");

        if(updateCount == 0) {
            // No updates to process
            return Vector.toArray(results);
        };

        // Iterate over each update result
        label proc for(idx in Iter.range(0, updateCount-1)) {
            debug d(debug_channel.announce, "                    SUBSCRIBER: updateSubscription processing result " # debug_show(idx));
            let updateResult = orchestratorResults[idx];
            let updateRequest = updates[idx];

            switch(updateResult){
                case(?#Ok(_)) {
                    // Successful update, apply changes to internal state
            
                    // Update by Subscription ID
                    let ?sub = BTree.get(state.subscriptions, Nat.compare, updateRequest.subscriptionId) else {
                        // Subscription not found
                        Vector.add<SubscriptionUpdateResult>(results, ?#Err(#NotFound));
                        continue proc;
                    };
                    
                  
                    Set.add(subsUpdated, Set.nhash, sub.id);
                        
                    // Indicate success
                    Vector.add(results, ?#Ok(true));
              
                        
                  
                };
                case(?#Err(err)) {
                    // Handle specific error returned from Orchestrator
                    debug d(debug_channel.announce, "                    SUBSCRIBER: Orchestrator returned error: " # debug_show(err));
                    Vector.add(results, ?#Err(err));
                };
                case(null) {
                    // Handle specific error returned from Orchestrator
                    debug d(debug_channel.announce, "                    SUBSCRIBER: Orchestrator returned null: ");
                    Vector.add(results, ?#Err(#GenericError { error_code = 0; message = "Orchestrator returned null" }));
                };
            };
        };

        debug d(debug_channel.announce, "                    SUBSCRIBER: updateSubscription completed with " # debug_show(Vector.size(results)) # " results");

        label lookup for(thisRecord in Set.keys(subsUpdated)){
          let ?subscription = BTree.get(state.subscriptions, Nat.compare, thisRecord) else continue lookup;

          let updatedConfig = await Orchestrator.icrc72_get_subscribers({
            prev = null;
            take = null;
            filter = ?{slice= [#ByNamespace(subscription.namespace), #BySubscriber(canister)]; statistics = null}});

          if(updatedConfig.size() != 1){
            debug d(debug_channel.announce, "                    SUBSCRIBER: updateSubscription failed to get updated config");
            continue lookup;
          };

          fileSubscription({
            id = subscription.id;
            config = updatedConfig[0].config;
            namespace = subscription.namespace;
          });
          
        };

        return Vector.toArray(results);
    };

    /**
     * Cancel active replay requests.
     *
     * @param replayIds - Array of replay IDs to cancel
     * @returns Array of cancellation results
     */
    public func cancelReplay(replayIds: [Nat]) : async [CancelReplayResult] {
        debug d(debug_channel.announce, "                    SUBSCRIBER: cancelReplay called for " # debug_show(replayIds));

        let result = try {
            await ICRC77Orchestrator.icrc77_cancel_replay(replayIds);
        } catch(e) {
            state.error := ?Error.message(e);
            debug d(debug_channel.announce, "                    SUBSCRIBER: Error canceling replay: " # Error.message(e));
            return [];
        };

        // Remove canceled replays from local state based on successful cancellations
        for(thisResult in result.vals()) {
            switch(thisResult) {
                case(?#Ok(replayId)) {
                    ignore BTree.delete(state.replays, Nat.compare, replayId);
                    ignore BTree.delete(state.replayStatus, Nat.compare, replayId);
                    debug d(debug_channel.announce, "                    SUBSCRIBER: Removed canceled replay " # debug_show(replayId));
                };
                case(?#Err(error)) {
                    debug d(debug_channel.announce, "                    SUBSCRIBER: Failed to cancel replay: " # debug_show(error));
                };
                case(null) {
                    debug d(debug_channel.announce, "                    SUBSCRIBER: Null result for cancel request");
                };
            };
        };

        debug d(debug_channel.announce, "                    SUBSCRIBER: cancelReplay completed with " # debug_show(result.size()) # " results");
        result;
    };

    /**
     * Get information about active replays.
     *
     * @returns Array of replay information including ID, namespace, broadcaster, status, etc.
     */
    public func getReplayInfo() : [(Nat, (Text, ?Principal, ?Text, ?(Nat, Nat), (Nat, ?Nat), ?(Nat, Nat)))] {
        debug d(debug_channel.announce, "                    SUBSCRIBER: getReplayInfo called");
        
        let replayArray = BTree.toArray(state.replays);
        let results = Array.map<(Nat, (Text, ?Principal, ?Text, ?(Nat, Nat), (Nat, ?Nat))), (Nat, (Text, ?Principal, ?Text, ?(Nat, Nat), (Nat, ?Nat), ?(Nat, Nat)))>(
            replayArray, 
            func((replayId, replayInfo) : (Nat, (Text, ?Principal, ?Text, ?(Nat, Nat), (Nat, ?Nat)))) : (Nat, (Text, ?Principal, ?Text, ?(Nat, Nat), (Nat, ?Nat), ?(Nat, Nat))) {
                let status = BTree.get(state.replayStatus, Nat.compare, replayId);
                (replayId, (replayInfo.0, replayInfo.1, replayInfo.2, replayInfo.3, replayInfo.4, status))
            }
        );
        
        debug d(debug_channel.announce, "                    SUBSCRIBER: getReplayInfo returning " # debug_show(results.size()) # " replays");
        results;
    };

    /**
     * Get status of a specific replay by ID.
     *
     * @param replayId - The replay ID to query
     * @returns Optional replay information
     */
    public func getReplayById(replayId: Nat) : ?(Text, ?Principal, ?Text, ?(Nat, Nat), (Nat, ?Nat), ?(Nat, Nat)) {
        debug d(debug_channel.announce, "                    SUBSCRIBER: getReplayById called for " # debug_show(replayId));
        
        switch(BTree.get(state.replays, Nat.compare, replayId)) {
            case(?replayInfo) {
                let status = BTree.get(state.replayStatus, Nat.compare, replayId);
                ?(replayInfo.0, replayInfo.1, replayInfo.2, replayInfo.3, replayInfo.4, status);
            };
            case(null) null;
        };
    };




    ///////////
    // ICRC85 ovs
    //////////

    private var _icrc85init = false;

    private func ensureCycleShare<system>() : async*(){
      if(_icrc85init == true) return;
      _icrc85init := true;

      ignore Timer.setTimer<system>(#nanoseconds(OneDay), scheduleCycleShare);
      environment.tt.registerExecutionListenerAsync(?"icrc85:ovs:shareaction:icrc72subscriber", handleIcrc85Action : TT.ExecutionAsyncHandler);
    };

    private func scheduleCycleShare<system>() : async() {
      //check to see if it already exists
      debug d(debug_channel.announce, "in schedule cycle share");
      switch(state.icrc85.nextCycleActionId){
        case(?val){
          switch(Map.get(environment.tt.getState().actionIdIndex, Map.nhash, val)){
            case(?_time) {
              //already in the queue
              return;
            };
            case(null) {};
          };
        };
        case(null){};
      };



      let result = environment.tt.setActionSync<system>(Int.abs(Time.now()), ({actionType = "icrc85:ovs:shareaction:icrc72subscriber"; params = Blob.fromArray([]);}));
      state.icrc85.nextCycleActionId := ?result.id;
    };

    private func handleIcrc85Action<system>(id: TT.ActionId, action: TT.Action) : async* Star.Star<TT.ActionId, TT.Error>{

      D.print("in handle timer async " # debug_show((id,action)));
      switch(action.actionType){
        case("icrc85:ovs:shareaction:icrc72subscriber"){
          await* shareCycles<system>();
          #awaited(id);
        };
        case(_) #trappable(id);
      };
    };

    private func shareCycles<system>() : async*(){
      debug d(debug_channel.announce, "in share cycles ");
      let lastReportId = switch(state.icrc85.lastActionReported){
        case(?val) val;
        case(null) 0;
      };

      debug d(debug_channel.announce, "last report id " # debug_show(lastReportId));

      let actions = if(state.icrc85.activeActions > 0){
        state.icrc85.activeActions;
      } else {1;};

      state.icrc85.activeActions := 0;

      debug d(debug_channel.announce, "actions " # debug_show(actions));

      var cyclesToShare = 1_000_000_000_000; //1 XDR

      if(actions > 0){
        let additional = Nat.div(actions, 10000);
        debug d(debug_channel.announce, "additional " # debug_show(additional));
        cyclesToShare := cyclesToShare + (additional * 1_000_000_000_000);
        if(cyclesToShare > 100_000_000_000_000) cyclesToShare := 100_000_000_000_000;
      };

      debug d(debug_channel.announce, "cycles to share" # debug_show(cyclesToShare));

      try{
        await* ovsfixed.shareCycles<system>({
          environment = do?{environment.advanced!.icrc85};
          namespace = "com.panindustrial.libraries.icrc72subscriber";
          actions = actions;
          schedule = func <system>(period: Nat) : async* (){
            let result = environment.tt.setActionSync<system>(Int.abs(Time.now()) + period, {actionType = "icrc85:ovs:shareaction:icrc72subscriber"; params = Blob.fromArray([]);});
            state.icrc85.nextCycleActionId := ?result.id;
          };
          cycles = cyclesToShare;
        });
      } catch(e){
        debug d(debug_channel.announce, "error sharing cycles" # Error.message(e));
      };

    };

    let OneDay =  86_400_000_000_000;

    

  };
}