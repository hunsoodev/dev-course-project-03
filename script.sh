#!/bin/bash
echo "test"
echo "Moving to the project directory..."
cd ~/dev-course-project-03 || { echo "Failed to change directory. Exiting."; exit 1; }

# 현재 Git 브랜치 이름을 추출하고 출력
echo "Retrieving the current Git branch..."
current_branch=$(git branch --show-current)
echo "Current branch is: $current_branch"

flag=false

# 만약 현재 브랜치가 'main'이 아니라면 git stash 실행
if [ "$current_branch" != "main" ]; then
  echo "Not on main branch. Executing git stash..."
  git stash
  echo "Switching to 'main' branch..."
  git switch main
  flag=true
else
    echo "On main branch. No action needed."
fi

echo "Fetching the latest changes from origin..."
git fetch --all || { echo "Failed to fetch changes. Exiting."; exit 1; }

echo "Checking for changes in 'docker-compose.yaml'..."
if git diff main origin/main --name-only | grep -q 'docker-compose.yaml'; then
  echo "'docker-compose.yaml' has changed."
  echo "Stopping and restarting docker-compose services..."
  docker-compose down && docker-compose up -d || { echo "Failed to restart docker-compose services. Exiting."; exit 1; }
else
  echo "No changes in 'docker-compose.yaml'."
fi

echo "Pulling the latest changes from Git..."
git pull || { echo "Failed to pull changes from Git. Exiting."; exit 1; }

# flag가 true일 경우 원래 브랜치로 돌아감
if [ "$flag" = true ]; then
  echo "Switching back to the original branch: $current_branch"

  # 원래 브랜치로 이동
  git switch $current_branch

  # stash에 저장된 변경사항을 복구
  git stash pop
fi
