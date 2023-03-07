IP=0.0.0.0
PORT=8765
TOKEN=c9b5f9e45cc8ec655ee9c6d871852978826bdfb33263b669
sudo docker stop psi
sudo docker rm psi
sudo docker run --name psi \
  --restart=always -d -p $PORT:8888 \
  -v /home/daniel/search-service:/tf/notebooks \
  tensorflow/tensorflow:2.9.1-gpu-jupyter\
  jupyter notebook \
  --notebook-dir=/tf/notebooks \
  --NotebookApp.token=$TOKEN \
  --ip $IP \
  --no-browser \
  --allow-root

docker exec -it psi python3 -m pip install -r /tf/notebooks/requirements.txt

echo http://localhost:$PORT/?token=$TOKEN

