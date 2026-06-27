#!/bin/bash
# 电影推荐系统 - API 接口自动化测试脚本
# 用法: bash test_api.sh [user_id]
# 默认 user_id=1

BASE_URL="http://localhost:3000"
USER_ID=${1:-1}

echo "=========================================="
echo "  电影推荐系统 API 测试"
echo "  服务器: $BASE_URL"
echo "  用户ID: $USER_ID"
echo "=========================================="

# ---------- 1. 登录测试 ----------
echo ""
echo "=== [1/6] 登录测试 ==="
LOGIN_RES=$(curl -s -X POST "$BASE_URL/api/login" \
  -H "Content-Type: application/json" \
  -d '{"username":"testuser1","password":"123456"}')
echo "$LOGIN_RES" | python3 -m json.tool 2>/dev/null || echo "$LOGIN_RES"

# ---------- 2. 随机电影测试 ----------
echo ""
echo "=== [2/6] 随机电影列表 (10部) ==="
MOVIE_RES=$(curl -s "$BASE_URL/api/random_movies?user_id=$USER_ID&count=10")
echo "$MOVIE_RES" | python3 -m json.tool 2>/dev/null || echo "$MOVIE_RES"
MOVIE_COUNT=$(echo "$MOVIE_RES" | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('movies',[])))" 2>/dev/null)
echo "→ 返回电影数量: $MOVIE_COUNT"

# ---------- 3. 提交评分测试 ----------
echo ""
echo "=== [3/6] 提交评分 ==="
# 取前3部随机电影评分
MOVIE_IDS=$(echo "$MOVIE_RES" | python3 -c "
import sys,json
movies = json.load(sys.stdin).get('movies',[])
ids = [str(m['movie_id']) for m in movies[:3]]
print(','.join(ids))
" 2>/dev/null)

RATING_BODY="{\"user_id\":$USER_ID,\"ratings\":["
FIRST=true
for MID in $(echo "$MOVIE_IDS" | tr ',' ' '); do
  if [ "$FIRST" = true ]; then
    RATING_BODY="$RATING_BODY{\"movie_id\":$MID,\"rating\":5}"
    FIRST=false
  else
    RATING_BODY="$RATING_BODY,{\"movie_id\":$MID,\"rating\":5}"
  fi
done
RATING_BODY="$RATING_BODY]}"

RATE_RES=$(curl -s -X POST "$BASE_URL/api/submit_ratings" \
  -H "Content-Type: application/json" \
  -d "$RATING_BODY")
echo "$RATE_RES" | python3 -m json.tool 2>/dev/null || echo "$RATE_RES"

# ---------- 4. 推荐接口测试（CF协同过滤）----------
echo ""
echo "=== [4/6] 推荐接口 (CF协同过滤) ==="
REC_RES=$(curl -s "$BASE_URL/api/recommend?user_id=$USER_ID&strategy=cf")
echo "$REC_RES" | python3 -m json.tool 2>/dev/null || echo "$REC_RES"

# ---------- 5. 推荐理由测试 ----------
echo ""
echo "=== [5/6] 推荐理由 ==="
REASON_RES=$(curl -s "$BASE_URL/api/recommend/reason?user_id=$USER_ID")
echo "$REASON_RES" | python3 -m json.tool 2>/dev/null || echo "$REASON_RES"

# ---------- 6. 用户偏好测试 ----------
echo ""
echo "=== [6/6] 用户偏好 ==="
PREF_RES=$(curl -s "$BASE_URL/api/user/preference?user_id=$USER_ID")
echo "$PREF_RES" | python3 -m json.tool 2>/dev/null || echo "$PREF_RES"

echo ""
echo "=========================================="
echo "  测试完成"
echo "=========================================="
