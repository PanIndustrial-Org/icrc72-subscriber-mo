module {

  public type ICRC16Property = {
        name : Text;
        value : ICRC16;
        immutable : Bool;
    };

    public type ICRC16 = {
        #Array : [ICRC16];
        #Blob : Blob;
        #Bool : Bool;
        #Bytes : [Nat8];
        #Class : [ICRC16Property];
        #Float : Float;
        #Floats : [Float];
        #Int : Int;
        #Int16 : Int16;
        #Int32 : Int32;
        #Int64 : Int64;
        #Int8 : Int8;
        #Map : ICRC16Map;
        #ValueMap : [(ICRC16, ICRC16)];
        #Nat : Nat;
        #Nat16 : Nat16;
        #Nat32 : Nat32;
        #Nat64 : Nat64;
        #Nat8 : Nat8;
        #Nats : [Nat];
        #Option : ?ICRC16;
        #Principal : Principal;
        #Set : [ICRC16];
        #Text : Text;
    };

    //ICRC3 Value
    public type Value = {
        #Nat : Nat;
        #Nat8 : Nat8;
        #Int : Int;
        #Text : Text;
        #Blob : Blob;
        #Bool : Bool;
        #Array : [Value];
        #Map : [(Text, Value)];
    };

    public type ICRC16Map = [(Text, ICRC16)];

  public type Namespace = Text;

  public type GenericError = {
    error_code: Nat;
    message: Text;
  };

  public type PublicationRegisterResult = ?{
    #Ok: Nat;
    #Err: PublicationRegisterError;
  };
  
  public type PublicationRegisterError = {
    #Unauthorized; //generally unauthorized
    #UnauthorizedPublisher : {
      namespace : Namespace; //The publisher is not allowed, Look up config by message: Text;
    };
    #ImproperConfig : Text; //maybe implementation specific
    #GenericError : GenericError;
    //validated
    #Exists : Nat; //The publication already exists and this is its number
    #GenericBatchError : Text;
  };

  public type PublicationRegistration = {
    namespace : Text; // The namespace of the publication for categorization and filtering
    config : ICRC16Map; // Additional configuration or metadata about the publication
    memo: ?Blob;
  };

  public type PublicationIdentifier = {
    #namespace: Text;
    #publicationId: Nat;
  };

  public type SubscriptionIdentifier = {
    #namespace: Text;
    #subscriptionId: Nat;
  };

  public type PublicationUpdateRequest = {
    publication : PublicationIdentifier;
    config : (Text, ICRC16);
    memo: ?Blob;
  };

  public type PublicationUpdateError = {
    #Unauthorized; //generally unauthorized
    #ImproperConfig : Text; //maybe implementation specific
    #GenericError : GenericError;
    #NotFound;
    #GenericBatchError : Text;
  };

  public type PublicationUpdateResult = ?{
     #Ok: Bool;
     #Err: PublicationUpdateError;
  };

  public type SubscriptionRegistration = {
    namespace : Text; // The namespace of the publication for categorization and filtering
    config : ICRC16Map; // Additional configuration or metadata about the publication
    memo: ?Blob;
  };

  public type SubscriptionRegisterResult = ?{
    #Ok: Nat;
    #Err: SubscriptionRegisterError;
  };

  public type SubscriptionRegisterError = {
    #Unauthorized; //generally unauthorized
    #ImproperConfig : Text; //maybe implementation specific
    #GenericError : GenericError;
    #PublicationNotFound;
    //validated
    #Exists : Nat; //The subscription already exists and this is its number
    #GenericBatchError : Text;
  };

  public type SubscriptionUpdateRequest = {
    subscription : {
      #namespace: Text;
      #id: Nat;
    };
    subscriber: ?Principal;
    config : (Text, ICRC16);
    memo: ?Blob;
  };

  public type SubscriptionUpdateError = {
    #Unauthorized; //generally unauthorized
    #ImproperConfig : Text; //maybe implementation specific
    #GenericError : GenericError;
    #NotFound;
    #GenericBatchError : Text;
  };

  public type SubscriptionUpdateResult = ?{
     #Ok: Bool;
     #Err: SubscriptionUpdateError;
  };

  public type Stats = ICRC16Map;


    public type PublicationInfo = {
      namespace: Text;
      publicationId: Nat;
      config: ICRC16Map;
      stats: Stats;
    };

    public type PublisherInfo = {
      publisher: Principal;
      stats: Stats;
    };

    //broken down by namespace
    public type PublisherPublicationInfo = {
      publisher: Principal;
      namespace: Text;
      publicationId: Nat;
      config: ICRC16Map;
      stats: Stats;
    };

  public type SubscriberSubscriptionInfo = {
    subscriptionId : Nat;
    subscriber: Principal;
    config: ICRC16Map;
    stats: Stats;
  };
  public type SubscriptionInfo = {
    subscriptionId: Nat;
    namespace: Text;
    config: ICRC16Map;
    stats: Stats;
  };

  public type SubscriberInfo = {
    subscriber: Principal;
    config: ICRC16Map;
    subscriptions: ?[Nat];
    stats: Stats;
  };

  public type BroadcasterInfo = {
    broadcaster: Principal;
    stats: Stats;
  };

  public type ICRC75Item = {
    principal: Principal;
    namespace: Namespace
  };

  public type ValidBroadcastersResponse = {
    #list: [Principal];
    #icrc75: ICRC75Item;
  };

  public type StatisticsFilter = ??[Text];

  public type OrchestrationQuerySlice = {
    #ByPublisher: Principal;
    #ByNamespace: Text;
    #BySubscriber: Principal;
    #ByBroadcaster: Principal;
  };

  public type OrchestrationFilter = {
    statistics: StatisticsFilter;
    slice: [OrchestrationQuerySlice];
  };

  public type PublicationDeleteRequest = {
    memo: ?Blob;
    publication: PublicationIdentifier;
  };

  public type SubscriptionDeleteRequest = {
    memo: ?Blob;
    subscription: SubscriptionIdentifier;
    subscriber: ?Principal;
  };

  public type PublicationDeleteResult = ?{
    #Ok: Bool;
    #Err: PublicationDeleteError;
  };

  public type SubscriptionDelete = {
    memo: Blob;
    subscription: SubscriptionIdentifier;
  };

  public type SubscriptionDeleteResult = ?{
    #Ok: Bool;
    #Err: SubscriptionDeleteError;
  };

  public type PublicationDeleteError = {
    #Unauthorized; //generally unauthorized
    #GenericError: GenericError;
    #NotFound;
    #GenericBatchError : Text;
  };

  public type SubscriptionDeleteError = {
    #Unauthorized; //generally unauthorized
    #GenericError: GenericError;
    #NotFound;
    #GenericBatchError : Text;
  };

 ///MARK: Constants
  public let CONST = {
    broadcasters = {
      sys = "icrc72:broadcaster:sys:";
      publisher = {
        broadcasters = {
          add = "icrc72:broadcaster:publisher:broadcaster:add";
          remove = "icrc72:broadcaster:publisher:broadcaster:remove";
        };
        add = "icrc72:broadcaster:publisher:add";
        remove = "icrc72:broadcaster:publisher:remove";
      };
      subscriber = {
        add = "icrc72:broadcaster:subscriber:add";
        remove = "icrc72:broadcaster:subscriber:remove";
      };
      relay = {
        add = "icrc72:broadcaster:relay:add";
        remove = "icrc72:broadcaster:relay:remove";
      };
      relayer = {
        add = "icrc72:broadcaster:relayer:add";
        remove = "icrc72:broadcaster:relayer:remove";
      };
    };
    subscription = {
      filter = "icrc72:subscription:filter";
      filter_update = "icrc72:subscription:filter:update";
      filter_remove = "icrc72:subscription:filter:remove";
      bStopped = "icrc72:subscription:bStopped";
      skip = "icrc72:subscription:skip";
      skip_update = "icrc72:subscription:skip:update";
      skip_remove = "icrc72:subscription:skip:remove";
      controllers = {
        list = "icrc72:subscription:controllers";
        list_add = "icrc72:subscription:controllers:list:add";
        list_remove = "icrc72:subscription:controllers:list:remove";
      };
    };

    publication = {
      actions = {
        canAssignBroadcaster = "icrc72:canAssignBroadcaster";
        assignBroadcasterToSubscriber = "icrc72:assignBroadcasterToSubscriber";
      };
      controllers = {
        list = "icrc72:publication:controllers";
        list_add = "icrc72:publication:controllers:list:add";
        list_remove = "icrc72:publication:controllers:list:remove";
      };
      publishers = {
        allowed = {
          list_add = "icrc72:publication:publishers:allowed:list:add";
          list_remove = "icrc72:publication:publishers:allowed:list:remove";
          list = "icrc72:publication:publishers:allowed:list";
          icrc75 = "icrc72:publication:publishers:allowed:icrc75";
          icrc75_remove = "icrc72:publication:publishers:allowed:icrc75:remove";
          icrc75_update = "icrc72:publication:publishers:allowed:icrc75:update";
        };
        disallowed = {
          list_add = "icrc72:publication:publishers:disallowed:list:add";
          list_remove = "icrc72:publication:publishers:disallowed:list:remove";
          list = "icrc72:publication:publishers:disallowed:list";
          icrc75 = "icrc72:publication:publishers:disallowed:icrc75";
          icrc75_remove = "icrc72:publication:publishers:disallowed:icrc75:remove";
          icrc75_update = "icrc72:publication:publishers:disallowed:icrc75:update";
        };
      };
      subscribers = {
        
        allowed = {
          list_add = "icrc72:publication:subscribers:allowed:list:add";
          list_remove = "icrc72:publication:subscribers:allowed:list:remove";
          list = "icrc72:publication:subscribers:allowed:list";
          icrc75 = "icrc72:publication:subscribers:allowed:icrc75";
          icrc75_remove = "icrc72:publication:subscribers:allowed:icrc75:remove";
          icrc75_update = "icrc72:publication:subscribers:allowed:icrc75:update";
        };
        disallowed = {
          list_add = "icrc72:publication:subscribers:disallowed:list:add";
          list_remove = "icrc72:publication:subscribers:disallowed:list:remove";
          list = "icrc72:publication:subscribers:disallowed:list";
          icrc75 = "icrc72:publication:subscribers:disallowed:icrc75";
          icrc75_remove = "icrc72:publication:subscribers:disallowed:icrc75:remove";
          icrc75_update = "icrc72:publication:subscribers:disallowed:icrc75:update";
        };
      };
      broadcasters = {
        sys = "icrc72:broadcaster:sys:";
        publisher = {
          add = "icrc72:broadcaster:publisher:add";
          remove = "icrc72:broadcaster:publisher:remove";
        };
        subscriber = {
          add = "icrc72:broadcaster:subscriber:add";
          remove = "icrc72:broadcaster:subscriber:remove";
        };
        relay = {
          add = "icrc72:broadcaster:relay:add";
          remove = "icrc72:broadcaster:relay:remove";
        };
        relayer = {
          add = "icrc72:broadcaster:relayer:add";
          remove = "icrc72:broadcaster:relayer:remove";
        };
      };
      created = "icrc72:publication:created";
    };
    publishers = {
      sys = "icrc72:publisher:sys:";
    };
    
    subscribers = {
      sys = "icrc72:subscriber:sys:";
    }
  };

  public type Service = actor {
    icrc72_register_subscription: ([SubscriptionRegistration]) -> async [SubscriptionRegisterResult];
    icrc72_register_publication: ([PublicationRegistration]) -> async [PublicationRegisterResult];
    icrc72_get_valid_broadcaster: () -> async ValidBroadcastersResponse;
    icrc72_get_publications: ({
      take: ?Nat;
      prev: ?Nat;
      filter: ?OrchestrationFilter;
    }) -> async [PublicationInfo];
    icrc72_get_subscriptions: ({
      take: ?Nat;
      prev: ?Nat;
      filter: ?OrchestrationFilter;
    }) -> async [SubscriptionInfo];
    icrc72_get_subscribers: ({
      take: ?Nat;
      prev: ?Nat;
      filter: ?OrchestrationFilter;
    }) -> async [SubscriberInfo];
    icrc72_update_publication: ([PublicationUpdateRequest]) -> async [PublicationUpdateResult];
    icrc72_update_subscription: ([SubscriptionUpdateRequest]) -> async [SubscriptionUpdateResult];
    icrc72_delete_publication: ([PublicationDeleteRequest]) -> async [PublicationDeleteResult];
    icrc72_delete_subscription: ([SubscriptionDeleteRequest]) -> async [SubscriptionDeleteResult];
  };
  
  

};