module {
  public let openvaluesharing = {
    platform = "icp";
    asset = "cycles";
    payment_mechanism = "icrc85_deposit_cycles_notify";
    custom = [
      {
        key = "namespace";
        value = #text("com.panindustrial.libraries.icrc72subscriber");
      },
      {
        key = "principal";
        value = #text("q26le-iqaaa-aaaam-actsa-cai");
      }
    ]
  };
};