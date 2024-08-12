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

  public type EventNotification = {
      id : Nat;
      eventId : Nat;
      prevEventId : ?Nat;
      timestamp : Nat;
      namespace : Text;
      data : ICRC16;
      source : Principal;
      headers : ?ICRC16Map;
      filter : ?Text;
  };
  
  public type Service = actor {
    icrc72_handle_notification: ([EventNotification]) -> ();
    icrc72_handle_notification_trusted: ([EventNotification]) -> async [?ICRC16];
  };

};