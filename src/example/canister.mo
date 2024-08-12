import Icrc72Orchestrator "../";

shared (deployer) actor class Example<system>()  = this {

  public shared func hello() : async Text {
    return "Hello, World!";
  };  

};