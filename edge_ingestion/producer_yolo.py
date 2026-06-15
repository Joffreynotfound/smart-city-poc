import json
import time
import argparse
from datetime import datetime
import cv2
from kafka import KafkaProducer
from ultralytics import YOLO

import ssl
ssl._create_default_https_context = ssl._create_unverified_context

# Configuration
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "urban.traffic"

def init_kafka_producer():
    """Initialise le producteur Kafka avec sérialisation JSON."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks=1,
        )
        print(f"[OK] Connecté au broker Kafka sur {KAFKA_BROKER}")
        return producer
    except Exception as e:
        print(f"[ERREUR] Impossible de se connecter à Kafka : {e}")
        exit(1)

def run_edge_pipeline(camera_id: str, video_path: str):
    producer = init_kafka_producer()
    print("[INFO] Chargement du modèle YOLOv8 nano...")
    model = YOLO("yolov8n.pt")

    # COCO 2=car, 3=motorcycle, 5=bus, 7=truck
    vehicle_classes = [2, 3, 5, 7]

    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        print(f"[ERREUR] Impossible d'ouvrir la vidéo : {video_path}")
        return

    fps = cap.get(cv2.CAP_PROP_FPS)
    time_between_frames = 1.0 / fps

    print(f"[OK] Démarrage du monitoring visuel. Appuyez sur 'q' pour quitter.")
    
    frame_count = 0

    while cap.isOpened():
        success, frame = cap.read()
        if not success:
            # Relecture en boucle de la vidéo
            cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
            frame_count = 0
            continue

        frame_count += 1
        start_time = time.time()

        # Inférence YOLO (Toutes les frames pour affichage fluide)
        results = model(frame, verbose=False)
        predictions = results[0].boxes

        # Affichage visuel (On trace les détections sur l'image)
        # La fonction .plot() dessine automatiquement les cadres et labels
        annotated_frame = results[0].plot()
        
        window_name = f"Smart City - {camera_id}"
        cv2.imshow(window_name, annotated_frame)

        # Envoi Kafka (Toutes les 10 frames pour le POC)
        if frame_count % 10 == 0:
            detected_vehicles = 0
            for box in predictions:
                cls_id = int(box.cls[0].item())
                if cls_id in vehicle_classes:
                    detected_vehicles += 1

            # Payload JSON
            payload = {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "camera_id": camera_id,
                "vehicle_count": detected_vehicles,
            }

            # Envoi asynchrone
            producer.send(TOPIC_NAME, value=payload)

            # Log clair dans la console (Toujours utile pour le mémoire)
            print(
                f"[{payload['timestamp']}] Envoi Kafka ➔ {detected_vehicles} véhicules."
            )

        # Contrôle de l'affichage (Fluidité et Quitter)
        # Petit calcul pour maintenir la vitesse réelle de la vidéo
        processing_time = time.time() - start_time
        delay = max(1, int((time_between_frames - processing_time) * 1000))

        # Attendre 'q' pour quitter ou la fin du delay
        if cv2.waitKey(delay) & 0xFF == ord('q'):
            print("[INFO] Arrêt demandé par l'utilisateur.")
            break

    # Nettoyage
    cap.release()
    cv2.destroyAllWindows()
    producer.close()
    print("[INFO] Pipeline Edge arrêté proprement.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Smart City — Edge Traffic Producer")
    parser.add_argument("--camera-id", default="CAM_ESIEE_01", help="Identifiant de la caméra")
    parser.add_argument("--video", default="traffic_highway_3.mp4", help="Chemin vers la vidéo source")
    args = parser.parse_args()

    run_edge_pipeline(camera_id=args.camera_id, video_path=args.video)