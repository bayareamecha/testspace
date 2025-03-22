#include <M5AtomS3.h>
#include <WiFi.h>
#include <WiFiMulti.h>
#include "secrets.h"
#include "freertos/task.h"
#include <Wire.h>
#include <SparkFun_MicroPressure.h>

WiFiMulti WiFiMulti;
WiFiClient client;

// Define a queue handle
QueueHandle_t pressureQueue;

// Flag to indicate Wi-Fi status
bool wifiConnected = false;
String dataPushStatus = "Not Connected";  // Updated by DataPush task

void DisplayTask(void* pvParameters) {
  float localPressureReading = 0.0;  // Local variable for the pressure reading
  while (1) {

    if (wifiConnected) {
      M5.Lcd.clear();
      M5.Lcd.setCursor(0, 0);
      M5.Lcd.setTextSize(1);
      M5.Lcd.setTextColor(TFT_GREEN, TFT_BLACK);
      M5.Lcd.printf("IP: %s\n", WiFi.localIP().toString().c_str());
      wifiConnected = false;  // Prevent repeated IP updates
    }

    // Check if a pressure value is available in the queue
    if (xQueueReceive(pressureQueue, &localPressureReading, portMAX_DELAY) == pdPASS) {
      // Only clear and update the display if pressure exceeds a threshold
      if (localPressureReading > 17) {
        M5.Lcd.clear();
        M5.Lcd.setTextColor(TFT_RED, TFT_BLACK);
        vTaskDelay(pdMS_TO_TICKS(250));  // Flash delay to refresh display
      }

      // Display the pressure value
      M5.Lcd.setCursor(0, 20);
      M5.Lcd.setTextSize(4);
      M5.Lcd.setTextColor(TFT_YELLOW, TFT_BLACK);
      M5.Lcd.printf("PSI:\r\n");
      M5.Lcd.setCursor(0, 60);
      M5.Lcd.printf("%.2f", localPressureReading);  // Display pressure with two decimal places

      // Display data push status
      M5.Lcd.setCursor(0, 100);
      M5.Lcd.setTextSize(1);
      if (dataPushStatus == "Connected     ") {
        M5.Lcd.setTextColor(TFT_GREEN, TFT_BLACK);
      } else {
        M5.Lcd.setTextColor(TFT_RED, TFT_BLACK);
      }
      M5.Lcd.printf("Server: %s", dataPushStatus.c_str());

    }
    // Add a slight delay to prevent task overload
    vTaskDelay(pdMS_TO_TICKS(500));  
  }
}

void DataPush(void* pvParameters) {
  float localPressureReading = 0.0;  // Local variable for the pressure reading

  // Random initial delay to stagger connections
  vTaskDelay(pdMS_TO_TICKS(random(0, 1000)));

  while (1) {
    // Check if a pressure value is available in the queue
    if (xQueueReceive(pressureQueue, &localPressureReading, pdMS_TO_TICKS(100)) == pdPASS) {
      // Attempt to connect to the server
      if (client.connect("10.10.40.30", 8888)) {
        String deviceIP = WiFi.localIP().toString();
        // Send the latest pressure reading to the server
        client.printf("Device IP: %s, Pressure: %.2f PSI\n", deviceIP.c_str(), localPressureReading);
        Serial.printf("Sent to server: Device IP: %s, Pressure: %.2f PSI\n", deviceIP.c_str(), localPressureReading);
        client.stop();

                // Update status to show successful connection
        dataPushStatus = "Connected     ";
      } else {
        Serial.println("Connection failed");
                // Update status to show failed connection
        dataPushStatus = "Not Connected";
      }
    }
    // Add a slight delay to control task execution frequency
    vTaskDelay(pdMS_TO_TICKS(1000));  // Delay for 1 second
  }
}

void I2cPoll(void* pvParameters) {
  SparkFun_MicroPressure mpr;

  int attempts = 0;
  while (!mpr.begin() && attempts++ < 5) {
    Serial.println("Retrying sensor initialization...");
    vTaskDelay(pdMS_TO_TICKS(500));
  }
  if (attempts >= 5) {
    Serial.println("Sensor initialization failed. Continuing without sensor.");
  }

  while (1) {
    float pressureReading = mpr.readPressure() -14.6959;  // Read pressure from the sensor
    if (pressureReading == -1) {
      Serial.println("Error reading sensor");
    }

    // Send the pressure reading to the display task
    if (xQueueSend(pressureQueue, &pressureReading, pdMS_TO_TICKS(100)) != pdPASS) {
      Serial.println("Queue is full. Data lost.");
    }

    vTaskDelay(pdMS_TO_TICKS(125));  // Non-blocking delay to send messages every 125ms
  }
}

void setup() {
    M5.begin();
    Wire.begin(2, 1);  // Use GPIO2 (SDA) and GPIO1 (SCL)

    // Init Wifi
    WiFiMulti.addAP(ssid, password);
    int retryCount = 0;
    while (WiFiMulti.run() != WL_CONNECTED) {
        M5.Lcd.print("..");
        delay(1000);
        if (++retryCount >= 8) {
          M5.Lcd.println("WiFi connection failed!");
          return;
        }
    }

      // Update the Wi-Fi status flag
    wifiConnected = true;

/* TASK EXAMPLE
    xTaskCreatePinnedToCore(
        task1,    // Function to implement the task.
                  // 
        "task1",  // 
        4096,     // The size of the task stack specified as the number of *
                  // bytes.
        NULL,     // Pointer that will be used as the parameter for the task *
                  // being created.  
        1,        // Priority of the task.  
        NULL,     // Task handler. 
        0);  // Core where the task should run.  
*/
//queue to store pressure readings
pressureQueue = xQueueCreate(1, sizeof(float));  // Create a queue with capacity for 10 float values

//tasks
xTaskCreatePinnedToCore(DisplayTask, "DisplayTask", 4096, NULL, 2, NULL, 0);
xTaskCreatePinnedToCore(DataPush, "DataPush", 4096, NULL, 2, NULL, 1);
xTaskCreatePinnedToCore(I2cPoll, "I2cPoll", 4096, NULL, 2, NULL, 1);
}

void loop() {
// FreeRTOS baby
}
