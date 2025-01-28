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

  /* public type PublicationRegisterResult = ?{
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
  };

  public type SubscriptionUpdateResult = ?{
     #Ok: Bool;
     #Err: SubscriptionUpdateError;
  };

  public type Stats = [ICRC16Map];


    public type PublicationInfo = {
      namespace: Text;
      publicationId: Nat;
      config: [ICRC16Map];
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
      config: [ICRC16Map];
      stats: Stats;
    };

  public type SubscriberSubscriptionInfo = {
    subscriptionId : Nat;
    subscriber: Principal;
    config: [ICRC16Map];
    stats: Stats;
  };


  public type SubscriptionInfo = {
    subscriptionId: Nat;
    namespace: Text;
    config: [ICRC16Map];
    stats: Stats;
  };

  public type SubscriberInfo = {
    subscriber: Principal;
    stats: Stats;
  };

  public type BroadcasterInfo = {
    broadcaster: Principal;
    stats: Stats;
  }; */

  public type Event = {
    eventId : Nat;
    prevEventId : ?Nat;
    timestamp : Nat;
    namespace : Text;
    source : Principal;
    data : ICRC16;
    headers : ?ICRC16Map;
  };

  public type PublishResult = {
    #Ok: [Nat]; //can be empty depending on implementation
    #Err: PublishError;
  };

  public type PublishError = {
    #PublicationNotFound;
    #Unauthorized;
    #GenericError : GenericError;
  };

  public type ConfirmError = {
    #NotificationNotFound;
    #EventNotFound;
    #Unauthorized;
    #GenericError : GenericError;
  };

  public type ConfirmMessageItemResult = {
    #Ok: Nat;
    #Err: ConfirmError;
  };

  public type ConfirmMessageResult = {
    #allAccepted;
    #itemized : [ConfirmMessageItemResult];
  };
  
  public type Service = actor {
    icrc72_publish: ([Event]) -> async [?PublishResult];
    icrc72_confirm_notifications: ([Nat]) -> async ConfirmMessageResult;
    //icrc72_get_valid_broadcaster: () -> async ValidBroadcastersResponse;
  };

};