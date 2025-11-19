import MigrationTypes "../types";
import D "mo:base/Debug";

module {
  public func upgrade(prevmigration_state: MigrationTypes.State, _args: MigrationTypes.Args, _caller: Principal): MigrationTypes.State {
    return #v0_0_0(#data);
  };
};