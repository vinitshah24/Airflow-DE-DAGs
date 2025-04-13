arg=$1
if [[ "${arg^^}" == "STOP" ]]; then
    echo "Shutting down docker containers!"
    rm -rf logs/*
    docker compose down
elif [[ "${arg^^}" == "START" ]]; then
    echo "Starting the docker containers!"
    # Start the docker containers (-d means run in detached mode)
    docker-compose up -d --remove-orphans
elif [[ "${arg^^}" == "CLEANUP" ]]; then
    echo "Cleaning up the docker containers!"
    rm -rf logs/*
    # Remove everything
    docker compose down --volumes --rmi all
elif [[ "${arg^^}" == "BUILD" ]]; then
    echo "Building docker containers!"
    docker-compose build
else
    echo "Running the docker container info!"
    docker-compose exec airflow-worker airflow info
fi

# login to the container [as root]
# docker exec -u root -it airflow-airflow-webserver-1 bash
