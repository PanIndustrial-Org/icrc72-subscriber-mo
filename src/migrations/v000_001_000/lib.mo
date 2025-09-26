import MigrationTypes "../types";
import Time "mo:base/Time";
import v0_1_0 "types";
import D "mo:base/Debug";

module {

  public let BTree = v0_1_0.BTree;
  public let Vector = v0_1_0.Vector;
  public let Set = v0_1_0.Set;
  public let Map = v0_1_0.Map;
  public type EmitableEvent = v0_1_0.EmitableEvent;

  public func upgrade(prevmigration_state: MigrationTypes.State, args: MigrationTypes.Args, caller: Principal): MigrationTypes.State {

    let (name) = switch (args) {
      case (?args) {(args.name)};
      case (_) {("nobody")};
    };

    let state : v0_1_0.State = {
      broadcasters = BTree.init<Nat, Vector.Vector<Principal>>(null);
      var validBroadcasters = #list(Set.new<Principal>());
      subscriptions = BTree.init<Nat, v0_1_0.SubscriptionRecord>(null);
      subscriptionsByNamespace = BTree.init<Text, Nat>(null);
      confirmAccumulator = BTree.init<Principal, Vector.Vector<(Nat,Nat)>>(null);
      lastEventId = BTree.init<Text, BTree.BTree<Nat, Nat>>(null); //Namespace, subscription, lastidused
      var confirmTimer = null;
      backlogs = BTree.init<Nat, BTree.BTree<Nat, v0_1_0.EventNotification>>(null);
      var error = null;
      var readyForSubscription = false;
      replays = BTree.init<Nat, (Text, ?Principal, ?Text, ?(Nat, Nat), (Nat, ?Nat))>(null); //namespace, broadcaster, filter, skip, range,
      replayStatus = BTree.init<Nat, (Nat, Nat)>(null);
      icrc85 = {
        var nextCycleActionId = null;
        var lastActionReported = null;
        var activeActions = 0;
      };
    };

    return #v0_1_0(#data(state));
  };
};