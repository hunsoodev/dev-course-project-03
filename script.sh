#!/bin/bash

echo "Moving to the project directory..."
cd ~/dev-course-project-03 || { echo "Failed to change directory. Exiting."; exit 1; }

echo "Switching to 'develop' branch..."
git switch develop || { echo "Failed to switch branch. Exiting."; exit 1; }

echo "Fetching the latest changes from origin..."
git fetch || { echo "Failed to fetch changes. Exiting."; exit 1; }

# 현재 Git 브랜치 이름을 추출하고 출력
echo "Retrieving the current Git branch..."
current_branch=$(git branch --show-current)
echo "Current branch is: $current_branch"

echo "Checking for changes in 'docker-compose.yaml'..."
if git diff HEAD..origin/develop --name-only | grep -q 'docker-compose.yaml'; then
    echo "'docker-compose.yaml' has changed."
    echo "Stopping and restarting docker-compose services..."
    docker-compose down && docker-compose up -d || { echo "Failed to restart docker-compose services. Exiting."; exit 1; }
else
    echo "No changes in 'docker-compose.yaml'."
fi

echo "Pulling the latest changes from Git..."
git pull || { echo "Failed to pull changes from Git. Exiting."; exit 1; }