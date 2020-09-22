package io.stargate.producer.kafka;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceListener;

/* Logic for registering the kafka producer as an OSGI bundle */
public class KafkaProducerActivator implements BundleActivator, ServiceListener {
  @Override
  public void start(BundleContext context) throws Exception {}

  @Override
  public void stop(BundleContext context) throws Exception {}

  @Override
  public void serviceChanged(ServiceEvent event) {}
}
