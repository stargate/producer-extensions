package io.stargate.producer.kafka.schema;

public enum OperationType {
  UPDATE("u"),
  DELETE("d");

  private String alias;

  OperationType(String alias) {

    this.alias = alias;
  }

  public String getAlias() {
    return alias;
  }
}
