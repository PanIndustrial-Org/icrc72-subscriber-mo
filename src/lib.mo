import MigrationTypes "migrations/types";
import Migration "migrations";
import BTree "mo:stableheapbtreemap/BTree";
import OrchestrationService "../../icrc72-orchestrator.mo/src/service";
import BroadcasterService "../../icrc72-broadcaster.mo/src/service";
import Star "mo:star/star";

import Buffer "mo:base/Buffer";
import Error "mo:base/Error";

import Int "mo:base/Int";
import Nat "mo:base/Nat";
import Principal "mo:base/Principal";
import Text "mo:base/Text";
import Time "mo:base/Time";
import Service "service";

import D "mo:base/Debug";
import Array "mo:base/Array";

module {

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
  public type EventNotification = MigrationTypes.Current.EventNotification;
  public type InitArgs = MigrationTypes.Current.InitArgs;
  public type SubscriptionRecord = MigrationTypes.Current.SubscriptionRecord;


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

  public class Subscriber(stored: ?State, canister: Principal, environment: Environment){

    let debug_channel = {
      var handleNotification = true;
      var startup = true;
      var announce = true;
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

    let self : Service.Service = actor(Principal.toText(canister));
    var defaultHandler : ?ExecutionItem = null;

    public var Orchestrator : OrchestrationService.Service = actor(Principal.toText(environment.icrc72OrchestratorCanister));

    private func natNow(): Nat{Int.abs(Time.now())};

    public func getState() : CurrentState {
      return state;
    };

    private func fileSubscription(item: SubscriptionRecord) : () {
      ignore BTree.insert(state.subscriptionsByNamespace, Text.compare, item.namespace, item.id);
      ignore BTree.insert(state.subscriptions, Nat.compare, item.id, item);
    };

    //add new subscription
    public func registerSubscriptions(subscriptions: [SubscriptionRegistration]): async [OrchestrationService.SubscriptionRegisterResult] {
      debug if(debug_channel.announce) D.print("                    SUBSCRIBER: registerSubscriptions " # debug_show(subscriptions));
      let result = await Orchestrator.icrc72_register_subscription(subscriptions);
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
            debug if(debug_channel.announce) D.print("                    SUBSCRIBER: registerSubscriptions error: " # debug_show(val));
            //todo: should we retry?
          };
          case(null){
            debug if(debug_channel.announce) D.print("                    SUBSCRIBER: registerSubscriptions error: null");
          }
        };
        idx += 1;
      };

      debug if(debug_channel.announce) D.print("                    SUBSCRIBER: registerSubscriptions result: " # debug_show(result));
      result;
    };

    private let executionListeners = Map.new<Text, ExecutionItem>();

    public func registerExecutionListenerSync(namespace: ?Text, handler: ExecutionHandler) : () {
        debug if(debug_channel.announce) D.print("                    SUBSCRIBER: registerExecutionListenerSync " # debug_show(namespace));
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

      debug if(debug_channel.announce) D.print("                    SUBSCRIBER: registerExecutionListenerAsync " # debug_show(finalNamespace));
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

      debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: icrc72_handle_notification " # debug_show(caller) # " " # debug_show(items));

      if(caller == canister and items.size()==1){

        debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: self call");

        //here we handle the self call with a single item
        let item = items[0];

        debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: item " # debug_show(item));

        let headerMap = switch(item.headers : ?ICRC16Map){
          case(null) Map.new<Text, ICRC16>();
          case(?val) Map.fromIter<Text, ICRC16>(val.vals(), Map.thash);
        };
      
        let namespace = item.namespace;
        //todo: check the order of reciept

        let handler = if(namespace == ""){
          switch(defaultHandler){
            case(?val) val;
            case(null) {
              debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: no handler found: " # namespace);
              return;
            };
          }} else {
            switch(Map.get<Text, ExecutionItem>(executionListeners, Map.thash, namespace)){
            case(?val) val;
            case(null) {
              debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: no handler found: " # namespace);
              return;
            };
          };
        };

        debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: handler found: " # debug_show(namespace));

        //note: if a non-awaited trap occurs in one of the handlers, these items will not be replayed and they will need to be recovered and replayed. todo: would it make sense to call a self awaiting function here with a future so that they all get queued up with state change?  Would they execute in the same round?

        //note: there is a notification if there is a handler for awaited errors so that they can be handled.

        //no need to try catch here because this is only called fro a capturable section of code later in this function
        switch(handler){
          case(#Sync(val)) {
            val<system>(item);
          };
          case(#Async(val)) {
            await* val<system>(item);
          };
        };

        debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: handler done: " # debug_show(namespace));

        let ?#Blob(broadcasterBlob) = Map.get(headerMap, Map.thash, "broadcaster") else {
          debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: no broadcaster found" # debug_show(item.headers));
          return;
        };

        debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: broadcaster found " # debug_show(Principal.fromBlob(broadcasterBlob)));


        let accumulator = switch(BTree.get(state.confirmAccumulator, Principal.compare, Principal.fromBlob(broadcasterBlob))){
          case(null){
            let newVector = Vector.new<Nat>();
            ignore BTree.insert(state.confirmAccumulator, Principal.compare, Principal.fromBlob(broadcasterBlob), newVector);
            newVector;
          };
          case(?val) {val};
        };

        Vector.add(accumulator, item.id);

        debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: accumulator " # debug_show(accumulator));

        
        return;
      } else {

        let subscriptionsHandled = Set.new<Nat>();

        //todo: check that the broadcaster is valid
        label proc for(item in items.vals()){
          //we quickly hand these to our self with awaits to be able to trap errors

          debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: processing item " # debug_show((item, BTree.toArray(state.subscriptionsByNamespace))));

          //find the subscription
          let ?subscriptionId = BTree.get(state.subscriptionsByNamespace, Text.compare, item.namespace) else {
            debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: no subscription found for namespace: " # item.namespace);
            continue proc;
          };

          debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: subscription found for namespace: " # item.namespace # " " # debug_show(subscriptionId));


          //check the order of receipt if desired
          let canProceed = switch(environment.handleEventOrder){
            case(?val) val<system>(state, environment, subscriptionId, item);
            case(null) true;
          };

          if(canProceed == false){
            debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: cannot proceed for item " # debug_show(item));
            let backlog = switch(BTree.get(state.backlogs, Nat.compare, subscriptionId)){
              case(?val) val;
              case(null) {
                let newMap = BTree.init<Nat, EventNotification>(null);
                ignore BTree.insert(state.backlogs, Nat.compare, subscriptionId, newMap);
                newMap;
              };
            };
            ignore BTree.insert<Nat, EventNotification>(backlog, Nat.compare, item.eventId, item);
            continue proc;
          };

          debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: can proceed for item " # debug_show(item));

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

          debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: headerMap " # debug_show(headerMap));

          debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: calling handle notification " # debug_show(item));

          //todo: do I need a limiter?
          try{
            self.icrc72_handle_notification([item]);
          } catch(e){
            debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: error in self notification " # Error.message(e));

            switch(environment.handleNotificationError){
              case(?val) val<system>(item, e);
              case(null) {};
            };
          };
        };

        debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: subscriptionsHandled before await" # debug_show(subscriptionsHandled));
        await secretWait();

        debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: subscriptionsHandled after await" # debug_show(subscriptionsHandled));

        //handle backlogs
        let backlogBuffer = Buffer.Buffer<EventNotification>(1);
        label procSub for(thisSub in Set.keys(subscriptionsHandled)){

          debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: processing backlog for " # debug_show(thisSub));

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
          var canProceed = switch(environment.handleEventOrder){
            case(?val) val<system>(state, environment, thisSub, testMin.1);
            case(null) true;
          };

          label continuous while (canProceed){
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
            
            
            canProceed := switch(environment.handleEventOrder){
              case(?val) val<system>(state, environment, thisSub, testMin.1);
              case(null) true;
            };
            min := BTree.min(backlog);
            switch(min){
              case(?val) {};
              case(null) {
                ignore BTree.delete(state.backlogs, Nat.compare, thisSub);
                continue procSub;
              };
            };
          };
        };

        debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: backlogBuffer " # debug_show(backlogBuffer.size()));

        if(backlogBuffer.size() > 0){
          debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: backlogBuffer " # debug_show(backlogBuffer.size()));
          self.icrc72_handle_notification(Buffer.toArray<EventNotification>(backlogBuffer));
        };
      };

      debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: icrc72_handle_notification done setting timer for drain");

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

    private func drainConfirmations(actionId: TT.ActionId, action: TT.Action) : async* Star.Star<TT.ActionId, TT.Error> {

      debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: drainConfirmations " # debug_show(actionId) # " " # debug_show(action));

      let proc = BTree.toArray(state.confirmAccumulator);
      BTree.clear(state.confirmAccumulator);
      state.confirmTimer := null;

      for(thisItem in proc.vals()){

        debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: confirmAccumulator " # debug_show(thisItem));
        let broadcaster : BroadcasterService.Service = actor(Principal.toText(thisItem.0));
        ignore await broadcaster.icrc72_confirm_notifications(Vector.toArray(thisItem.1));
      };
      return #awaited(actionId);
    };

    private func validateBroadcaster(caller: Principal) : async* Bool {
      debug if(debug_channel.announce) D.print("                    SUBSCRIBER: validateBroadcaster " # debug_show(caller));
      switch(state.validBroadcasters){
        case(#list(val)) {
          return Set.has(val, phash, caller);
        };
        case(#icrc75(val)) {
          //todo: implement icrc75
          return false;
        };
      };
    };

    public func fileBroadcaster(broadcaster: Principal, subscriptionId : Nat, namespace: Text){

      debug if(debug_channel.announce) D.print("                    SUBSCRIBER: fileBroadcaster from subscriber" # debug_show(broadcaster) # " " # debug_show(subscriptionId) # " " # debug_show(namespace));

      let broadcasters = switch(BTree.get(state.broadcasters, Nat.compare, subscriptionId)){
          case(null) {
            let col = Vector.new<Principal>();
            ignore BTree.insert(state.broadcasters, Nat.compare, subscriptionId, col);
            col
          };
          case(?val) {val};
        };

        switch(Vector.indexOf<Principal>(broadcaster, broadcasters, Principal.equal)){
          case(?val) {};
          case(null) {
            Vector.add(broadcasters, broadcaster);
          };
        };
    };


    //life cycle
    private func handleBroadcasterEvents<system>(notification: EventNotification) : async* (){

      debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: handleBroadcasterEvents " # debug_show(notification));

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
            debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: invalid broadcaster");
            return;
          };

          debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: broadcaster validated");

          let #Array(newData) = thisItem.1 else return;

          for(thisAdd in newData.vals()){

            debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: broadcaster add " # debug_show(thisAdd));
            let #Array(itemData) = thisAdd else return;
            let #Text(subscriptionNamespace) = itemData[0] else return;
            let #Blob(principalBlob) = itemData[1] else return;
            let principal = Principal.fromBlob(principalBlob);

            let subscriptionId = switch(BTree.get(state.subscriptionsByNamespace, Text.compare, subscriptionNamespace)){
              case(?val) val;
              case(null) {
                debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: no subscription found for namespace: " # subscriptionNamespace);
                return;
              };
            };

            fileBroadcaster(principal, subscriptionId, subscriptionNamespace);
          };  
          
        } else if(data[0].0 == CONST.subscriber.broadcasters.remove){
          //todo: fix later
          /* debug if(debug_channel.handleNotification) D.print("                    SUBSCRIBER: broadcaster remove");
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
        };
        
      };
    };

    public type SubscribeRequestItem = {
      namespace : Text;
      config : ICRC16Map;
      memo : ?Blob;
      listener : ExecutionItem;
    };

    public type SubscribeRequest = [SubscribeRequestItem];

    public func subscribe(request: SubscribeRequest) : async* [OrchestrationService.SubscriptionRegisterResult] {
      debug if(debug_channel.announce) D.print("                    SUBSCRIBER: subscribe " # debug_show(request.size()));

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

      debug if(debug_channel.announce) D.print("                    SUBSCRIBER: subscribe result " # debug_show(result));
      result;
    };

    var _isInit = false;

    //register subscription for the system events
    public func initSubscriber() : async() {
      
      if(_isInit == true) return;
      _isInit := true;
      debug if(debug_channel.startup) D.print("                    SUBSCRIBER: initSubscriber");

      registerExecutionListenerAsync(?(CONST.subscriber.sys # Principal.toText(canister)), handleBroadcasterEvents);

      environment.tt.registerExecutionListenerAsync(?CONST.subscriber.timers.sendConfirmations, drainConfirmations); 

      let subscriptionResult = await registerSubscriptions([{
        namespace = CONST.subscriber.sys # Principal.toText(canister);
        config = [];
        memo = null
      }]);

      debug if(debug_channel.startup) D.print("                    SUBSCRIBER: subscriptionResult " # debug_show(subscriptionResult));

      //todo: check for valid broadcasters
      let validBroadcasters = try{
        await Orchestrator.icrc72_get_valid_broadcaster();
      } catch(e){
        //how to dea with this?
        state.error := ?Error.message(e);
        return;
      };

      debug if(debug_channel.startup) D.print("                    SUBSCRIBER: valid broadcasters " # debug_show(validBroadcasters));

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
  };
}