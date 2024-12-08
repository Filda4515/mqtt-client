package utb.fai;

import java.util.Timer;
import java.util.TimerTask;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import utb.fai.API.HumiditySensor;
import utb.fai.API.IrrigationSystem;

/**
 * Trida MQTT klienta pro mereni vhlkosti pudy a rizeni zavlazovaciho systemu. 
 * 
 * V teto tride implementuje MQTT klienta
 */
public class SoilMoistureMQTTClient {

    private MqttClient client;
    private HumiditySensor humiditySensor;
    private IrrigationSystem irrigationSystem;

    private Timer irrigationTimer;

    /**
     * Vytvori instacni tridy MQTT klienta pro mereni vhlkosti pudy a rizeni
     * zavlazovaciho systemu
     * 
     * @param sensor     Senzor vlhkosti
     * @param irrigation Zarizeni pro zavlahu pudy
     */
    public SoilMoistureMQTTClient(HumiditySensor sensor, IrrigationSystem irrigation) {
        this.humiditySensor = sensor;
        this.irrigationSystem = irrigation;
    }

    /**
     * Metoda pro spusteni klienta
     */
    public void start() {
        try {
            client = new MqttClient(Config.BROKER, Config.CLIENT_ID);
            client.connect();

            // IN
            client.subscribe(Config.TOPIC_IN);
            client.setCallback(new MqttCallback() {
                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    String msg = new String(message.getPayload());

                    if (msg.equals(Config.REQUEST_GET_HUMIDITY)) {
                        float humidity = humiditySensor.readRAWValue();
                        String humidityMessage = Config.RESPONSE_HUMIDITY + ";" + humidity;
                        client.publish(Config.TOPIC_OUT, new MqttMessage(humidityMessage.getBytes()));
                    } else if (msg.equals(Config.REQUEST_GET_STATUS)) {
                        String irrigationStatus = irrigationSystem.isActive() ? "irrigation_on" : "irrigation_off";
                        String statusMessage = Config.RESPONSE_STATUS + ";" + irrigationStatus;
                        client.publish(Config.TOPIC_OUT, new MqttMessage(statusMessage.getBytes()));
                    } else if (msg.equals(Config.REQUEST_START_IRRIGATION)) {
                        if (!irrigationSystem.isActive()) {
                            irrigationSystem.activate();
                            if (irrigationSystem.hasFault()) {
                                String faultMessage = "fault;IRRIGATION_SYSTEM";
                                client.publish(Config.TOPIC_OUT, new MqttMessage(faultMessage.getBytes()));
                                return;
                            }
                            String irrigationOnMessage = "status;irrigation_on";
                            client.publish(Config.TOPIC_OUT, new MqttMessage(irrigationOnMessage.getBytes()));

                            if (irrigationTimer != null) {
                                irrigationTimer.cancel();
                            }
                            irrigationTimer = new Timer();
                            irrigationTimer.schedule(new TimerTask() {
                                @Override
                                public void run() {
                                    try {
                                        irrigationSystem.deactivate();
                                        if (irrigationSystem.hasFault()) {
                                            String faultMessage = "fault;IRRIGATION_SYSTEM";
                                            client.publish(Config.TOPIC_OUT, new MqttMessage(faultMessage.getBytes()));
                                            return;
                                        }
                                        String irrigationOnMessage = "status;irrigation_off";
                                        client.publish(Config.TOPIC_OUT, new MqttMessage(irrigationOnMessage.getBytes()));
                                    } catch (MqttException e) {
                                        e.printStackTrace();
                                    }
                                }
                            }, 30000);
                        }
                    } else if (msg.equals(Config.REQUEST_STOP_IRRIGATION)) {
                        irrigationSystem.deactivate();
                        if (irrigationSystem.hasFault()) {
                            String faultMessage = "fault;IRRIGATION_SYSTEM";
                            client.publish(Config.TOPIC_OUT, new MqttMessage(faultMessage.getBytes()));
                            return;
                        }
                        String irrigationOffMessage = "status;irrigation_off";
                        client.publish(Config.TOPIC_OUT, new MqttMessage(irrigationOffMessage.getBytes()));

                        if (irrigationTimer != null) {
                            irrigationTimer.cancel();
                        }
                    }
                }
                
                @Override
                public void connectionLost(Throwable cause) {}
                
                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {}
            });

            // OUT
            Timer timer = new Timer();
            timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    try {
                        float humidity = humiditySensor.readRAWValue();
                        if (humiditySensor.hasFault()) {
                            String faultMessage = "fault;HUMIDITY_SENSOR";
                            client.publish(Config.TOPIC_OUT, new MqttMessage(faultMessage.getBytes()));
                            return;
                        }
                        String humidityMessage = Config.RESPONSE_HUMIDITY + ";" + humidity;

                        client.publish(Config.TOPIC_OUT, new MqttMessage(humidityMessage.getBytes()));
                    } catch (MqttException e) {
                        e.printStackTrace();
                    }
                }
            }, 0, 10000);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }
}
