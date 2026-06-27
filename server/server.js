const express = require('express');
const cors = require('cors');
const mysql = require('mysql2/promise');
const crypto = require('crypto');
const { execFile } = require('child_process');
const path = require('path');
const fs = require('fs');
const app = express();
const PORT = Number(process.env.PORT) || 3000;

const DB_CONFIG = {
  host: process.env.DB_HOST || 'localhost',
  port: Number(process.env.DB_PORT) || 3306,
  user: process.env.DB_USER || 'movieapp',
  password: process.env.DB_PASSWORD || 'movieapp123',
  database: process.env.DB_NAME || 'movie_recommend'
};

const HDFS_RATINGS = process.env.HDFS_RATINGS || '/user/fsl/movie_recommend/ratings.txt';
const PYSPARK_PYTHON = process.env.PYSPARK_PYTHON || 'python3';
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || '123456';

app.use(cors());
app.use(express.json());
app.use(express.static('public'));

const pool = mysql.createPool({
  ...DB_CONFIG,
  waitForConnections: true,
  connectionLimit: 10,
  charset: 'utf8mb4'
});

function findSparkSubmit() {
  const candidates = [
    '/opt/spark/bin/spark-submit',
    '/usr/local/spark/bin/spark-submit',
    '/home/fsl/spark/bin/spark-submit',
    'spark-submit'
  ];
  for (const c of candidates) {
    if (fs.existsSync(c)) return c;
  }
  return 'spark-submit';
}

const SPARK_SUBMIT = process.env.SPARK_SUBMIT || findSparkSubmit();
const ALS_SCRIPT = path.join(__dirname, 'spark', 'als_recommend_new.py');
const sparkCache = {};
let sparkBusy = false;

function shellQuote(value) {
  return "'" + String(value).replace(/'/g, "'\\''") + "'";
}

function syncRatingsToHDFS() {
  return new Promise((resolve) => {
    const tmpFile = '/tmp/filmradar_ratings.tsv';
    const exportCmd = [
      'MYSQL_PWD=' + shellQuote(DB_CONFIG.password),
      'mysql --protocol=TCP',
      '-h ' + shellQuote(DB_CONFIG.host),
      '-P ' + shellQuote(DB_CONFIG.port),
      '-u ' + shellQuote(DB_CONFIG.user),
      shellQuote(DB_CONFIG.database),
      '-B -e "SELECT user_id, movie_id, rating FROM ratings"',
      '> ' + shellQuote(tmpFile),
      '2>/dev/null'
    ].join(' ');

    execFile('bash', ['-c', exportCmd], { timeout: 15000 }, (err) => {
      if (err) {
        console.error('[HDFS] MySQL export failed:', err.message);
        return resolve(false);
      }
      fs.stat(tmpFile, (statErr, stat) => {
        if (statErr || stat.size === 0) {
          console.error('[HDFS] Exported file empty or missing');
          return resolve(false);
        }
        const hdfsCmd = 'hdfs dfs -put -f ' + shellQuote(tmpFile) + ' ' + shellQuote(HDFS_RATINGS);
        execFile('bash', ['-c', hdfsCmd], { timeout: 30000 }, (upErr, upStdout, upStderr) => {
          if (upErr) {
            console.error('[HDFS] Upload failed:', upErr.message, upStderr);
            fs.unlink(tmpFile, () => {});
            return resolve(false);
          }
          console.log('[HDFS] ratings.txt uploaded (' + stat.size + ' bytes)');
          fs.unlink(tmpFile, () => {});
          resolve(true);
        });
      });
    });
  });
}

function runSparkALS(userId) {
  return new Promise(async (resolve, reject) => {
    await syncRatingsToHDFS();
    const env = { ...process.env, PYSPARK_PYTHON };
    const args = [
      '--master', process.env.SPARK_MASTER || 'local[*]',
      ALS_SCRIPT,
      '--user-id', String(userId),
      '--db-host', DB_CONFIG.host,
      '--db-port', String(DB_CONFIG.port),
      '--db-user', DB_CONFIG.user,
      '--db-password', DB_CONFIG.password,
      '--db-name', DB_CONFIG.database,
      '--hdfs-ratings', HDFS_RATINGS
    ];
    const logFile = '/tmp/spark_als_' + userId + '_' + Date.now() + '.log';
    execFile(SPARK_SUBMIT, args, {
      env: env,
      maxBuffer: 10 * 1024 * 1024,
      timeout: 300000
    }, (error, stdout, stderr) => {
      const log = [
        '=== Spark ALS Log ===',
        'Time: ' + new Date().toISOString(),
        'User: ' + userId,
        '',
        '--- stdout ---',
        stdout,
        '--- stderr ---',
        stderr,
        '--- result ---',
        error ? 'FAILED: ' + error.message : 'SUCCESS'
      ].join('\n');
      fs.writeFile(logFile, log, () => {});

      if (error) {
        console.error('[Spark] Failed:', error.message);
        if (stderr) console.error('[Spark stderr]', stderr.slice(-500));
        reject(error);
        return;
      }
      console.log('[Spark] Success:', stdout.slice(-300));
      resolve(stdout);
    });
  });
}

const movieDetails = {};
for (let i = 1; i <= 215; i++) {
  movieDetails[i] = { description: '暂无简介', director: '未知' };
}
Object.assign(movieDetails, {
  1: { description: '一个银行家被冤枉入狱，在监狱中经历友情与希望的故事。', director: '弗兰克·德拉邦特' },
  2: { description: '两个京剧演员在动荡时代中的爱恨纠缠。', director: '陈凯歌' },
  3: { description: '一个智商不高的男子用善良与奔跑改变命运。', director: '罗伯特·泽米吉斯' },
  4: { description: '穷画家与富家女在泰坦尼克号上相遇的凄美爱情。', director: '詹姆斯·卡梅隆' },
  5: { description: '少女千寻误入神灵世界，为了救回父母展开冒险。', director: '宫崎骏' },
  6: { description: '盗梦者进入他人梦境窃取秘密，层层嵌套的科幻经典。', director: '克里斯托弗·诺兰' },
  7: { description: '为了人类未来，宇航员穿越虫洞寻找新家园。', director: '克里斯托弗·诺兰' },
  8: { description: '二战期间，德国商人辛德勒拯救犹太人的真实历史。', director: '史蒂文·斯皮尔伯格' },
  9: { description: '职业杀手与小女孩之间的温情与复仇故事。', director: '吕克·贝松' },
  10: { description: '忠犬八公在主人去世后依然每天等待的感人故事。', director: '拉斯·霍尔斯道姆' },
  11: { description: '一个天才钢琴师在船上度过一生的传奇。', director: '朱塞佩·托纳多雷' },
  12: { description: '三个大学生反抗填鸭式教育的幽默励志片。', director: '拉库马·希拉尼' },
  13: { description: '音乐老师用音乐改变问题少年们的命运。', director: '克里斯托夫·巴拉蒂' },
  14: { description: '至尊宝穿越时空寻找爱情的奇幻喜剧。', director: '刘镇伟' },
  15: { description: '黑手党家族的权力争斗与父子情仇。', director: '弗朗西斯·福特·科波拉' },
  16: { description: '蝙蝠侠与小丑的巅峰对决，探讨正义与混乱。', director: '克里斯托弗·诺兰' },
  17: { description: '一个普通人发现自己的生活被全天候直播的黑色喜剧。', director: '彼得·威尔' },
  18: { description: '两个小学生青涩纯真的初恋故事。', director: '罗伯·莱纳' },
  19: { description: '福贵一家在历史变迁中的生存与坚持。', director: '张艺谋' },
  20: { description: '孤儿哈利·波特进入魔法学校，揭开身世之谜。', director: '克里斯·哥伦布' },
  21: { description: '霍比特人弗罗多踏上销毁魔戒的史诗旅程。', director: '彼得·杰克逊' },
  22: { description: '少年与孟加拉虎在海难后漂泊求生的奇幻冒险。', director: '李安' },
  23: { description: '老人用气球实现亡妻遗愿的温馨动画。', director: '彼特·道格特' },
  24: { description: '单亲爸爸带着儿子追逐美国梦的真实奋斗史。', director: '加布里尔·穆奇诺' },
  25: { description: '动物大都会中兔子和狐狸联手破案的喜剧动画。', director: '拜伦·霍华德' },
  26: { description: '前摔跤手将女儿训练成世界冠军的励志运动片。', director: '尼特什·提瓦瑞' },
  27: { description: '白血病患者走私印度仿制药的真实事件改编。', director: '文牧野' },
  28: { description: '哪吒逆天改命的中国神话动画。', director: '饺子' },
  29: { description: '太阳系即将毁灭，人类带着地球逃离的科幻灾难片。', director: '郭帆' },
  30: { description: '男女高中生交换身体的奇幻爱情动画。', director: '新海诚' }
});

app.post('/api/register', async (req, res) => {
  try {
    const { username, password } = req.body;
    if (!username || !password) return res.json({ code: 1, msg: '用户名或密码不能为空' });
    const [exists] = await pool.query('SELECT user_id FROM users WHERE username = ?', [username]);
    if (exists.length > 0) return res.json({ code: 1, msg: '用户名已存在' });
    const md5 = crypto.createHash('md5').update(password).digest('hex');
    await pool.query('INSERT INTO users (username, password) VALUES (?, ?)', [username, md5]);
    res.json({ code: 0, msg: '注册成功' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ code: 1, msg: '注册失败' });
  }
});

app.post('/api/login', async (req, res) => {
  try {
    const { username, password } = req.body;
    if (!username || !password) return res.json({ code: 1, msg: '用户名或密码不能为空' });
    const md5 = crypto.createHash('md5').update(password).digest('hex');
    const [users] = await pool.query(
      'SELECT user_id, username, is_admin FROM users WHERE username = ? AND password = ?',
      [username, md5]
    );
    if (users.length === 0) return res.json({ code: 1, msg: '用户名或密码错误' });
    res.json({
      code: 0,
      msg: '登录成功',
      user: {
        user_id: users[0].user_id,
        username: users[0].username,
        is_admin: users[0].is_admin
      }
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ code: 1, msg: '登录失败' });
  }
});

app.get('/api/random_movies', async (req, res) => {
  try {
    const userId = parseInt(req.query.user_id) || 1;
    const count = parseInt(req.query.count) || 10;
    const [rated] = await pool.query('SELECT movie_id FROM ratings WHERE user_id = ?', [userId]);
    const ratedIds = rated.map(r => r.movie_id);
    let sql = 'SELECT movie_id, title, poster_url, genres, avg_rating FROM movies';
    if (ratedIds.length > 0) sql += ' WHERE movie_id NOT IN (?)';
    sql += ' ORDER BY RAND() LIMIT ?';
    if (ratedIds.length > 0) {
      const [movies] = await pool.query(sql, [ratedIds, count]);
      res.json({ movies });
    } else {
      const [movies] = await pool.query(sql, [count]);
      res.json({ movies });
    }
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '获取随机电影失败' });
  }
});

app.post('/api/submit_ratings', async (req, res) => {
  try {
    const { user_id, ratings } = req.body;
    if (!ratings || ratings.length === 0) return res.json({ code: 1, msg: '评分不能为空' });
    for (const r of ratings) {
      await pool.query(
        'INSERT INTO ratings (user_id, movie_id, rating, timestamp) VALUES (?, ?, ?, UNIX_TIMESTAMP()) ON DUPLICATE KEY UPDATE rating = VALUES(rating), timestamp = UNIX_TIMESTAMP()',
        [user_id, r.movie_id, r.rating]
      );
    }
    const movieIds = ratings.map(r => r.movie_id);
    for (const movieId of movieIds) {
      await pool.query(
        'UPDATE movies m SET avg_rating = (SELECT ROUND(AVG(rating), 1) FROM ratings WHERE movie_id = ?) WHERE m.movie_id = ?',
        [movieId, movieId]
      );
    }
    delete sparkCache[user_id];
    console.log('[Rec] Cache cleared for user', user_id, 'after batch ratings');
    res.json({ code: 0, msg: '评分提交成功' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '评分提交失败' });
  }
});

app.get('/api/movies', async (req, res) => {
  try {
    const search = req.query.search || '';
    const page = parseInt(req.query.page) || 1;
    const size = parseInt(req.query.size) || 20;
    const offset = (page - 1) * size;
    const [[{ total }]] = await pool.query(
      'SELECT COUNT(*) AS total FROM movies WHERE title LIKE ?',
      [`%${search}%`]
    );
    const [rows] = await pool.query(
      'SELECT movie_id, title, genres, avg_rating, year, poster_url FROM movies WHERE title LIKE ? LIMIT ? OFFSET ?',
      [`%${search}%`, size, offset]
    );
    const totalPages = Math.ceil(total / size);
    res.json({ total, page, size, totalPages, movies: rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '查询电影失败' });
  }
});

app.get('/api/recommend', async (req, res) => {
  try {
    const userId = parseInt(req.query.user_id) || 1;
    const strategy = req.query.strategy || 'cf';
    const forceRefresh = req.query.force_refresh === 'true';
    let recRows;

    if (strategy === 'cf') {
      const [ratingCount] = await pool.query(
        'SELECT COUNT(*) as cnt FROM ratings WHERE user_id = ?', [userId]
      );
      const cnt = ratingCount[0].cnt;

      if (cnt > 0) {
        await genreWeightedRecommend(userId);
      }

      if (cnt >= 5) {
        const shouldRunALS = (forceRefresh || !sparkCache[userId]) && !sparkBusy;
        if (shouldRunALS) {
          sparkBusy = true;
          sparkCache[userId] = Date.now();
          runSparkALS(userId).then(() => {
            sparkBusy = false;
          }).catch(sparkErr => {
            sparkBusy = false;
            console.log('[Spark] ALS failed:', sparkErr.message);
          });
          console.log('[Spark] Triggered ALS for user', userId, '(async)');
        } else {
          console.log('[Spark] Cached/skip ALS for user', userId);
        }
      }
      recRows = await hybridRerank(userId);
    } else if (strategy === 'hot') {
      [recRows] = await pool.query(
        'SELECT movie_id, title, genres, poster_url, avg_rating AS score FROM movies ORDER BY avg_rating DESC LIMIT 5'
      );
    } else if (strategy === 'new') {
      [recRows] = await pool.query(
        'SELECT movie_id, title, genres, poster_url, year, avg_rating AS score FROM movies ORDER BY year DESC LIMIT 5'
      );
    } else if (strategy === 'random') {
      const [rated] = await pool.query('SELECT movie_id FROM ratings WHERE user_id = ?', [userId]);
      const ratedIds = rated.map(r => r.movie_id);
      let sql = 'SELECT movie_id, title, genres, poster_url, avg_rating AS score FROM movies';
      if (ratedIds.length > 0) sql += ' WHERE movie_id NOT IN (?)';
      sql += ' ORDER BY RAND() LIMIT 5';
      if (ratedIds.length > 0) [recRows] = await pool.query(sql, [ratedIds]);
      else [recRows] = await pool.query(sql);
    }

    res.json({ user_id: userId, strategy, movies: recRows || [] });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '查询推荐失败' });
  }
});

function splitGenres(genresStr) {
  if (!genresStr) return [];
  if (genresStr.includes('|')) return genresStr.split('|').map(g => g.trim()).filter(Boolean);
  if (genresStr.includes(',')) return genresStr.split(',').map(g => g.trim()).filter(Boolean);
  return [genresStr.trim()].filter(Boolean);
}

async function hybridRerank(userId) {
  const [userRatings] = await pool.query(
    'SELECT r.rating, m.genres FROM ratings r JOIN movies m ON r.movie_id = m.movie_id WHERE r.user_id = ?',
    [userId]
  );

  const genrePref = {};
  for (const r of userRatings) {
    splitGenres(r.genres).forEach(g => {
      const key = g.trim();
      if (!key) return;
      if (!genrePref[key]) genrePref[key] = { total: 0, count: 0 };
      genrePref[key].total += r.rating;
      genrePref[key].count += 1;
    });
  }
  for (const key of Object.keys(genrePref)) {
    if (genrePref[key].count < 2) delete genrePref[key];
  }

  const [alsCandidates] = await pool.query(
    `SELECT r.score, m.movie_id, m.title, m.genres, m.poster_url, m.avg_rating
     FROM rec_movies r JOIN movies m ON r.movie_id = m.movie_id
     WHERE r.user_id = ?`, [userId]
  );

  const [regionCandidates] = await pool.query(
    `SELECT m.movie_id, m.title, m.genres, m.poster_url, m.avg_rating, 0 AS score
     FROM movies m WHERE m.movie_id NOT IN (
       SELECT movie_id FROM ratings WHERE user_id = ?
     )`, [userId]
  );

  const seenIds = new Set();
  const merged = [];
  for (const c of alsCandidates) {
    if (!seenIds.has(c.movie_id)) {
      seenIds.add(c.movie_id);
      merged.push(c);
    }
  }
  for (const c of regionCandidates) {
    if (!seenIds.has(c.movie_id)) {
      seenIds.add(c.movie_id);
      merged.push(c);
    }
  }

  if (merged.length === 0) {
    const [popular] = await pool.query(
      'SELECT movie_id, title, genres, poster_url, avg_rating AS score FROM movies ORDER BY avg_rating DESC LIMIT 5'
    );
    return popular;
  }

  const scored = merged.map(c => {
    const genres = splitGenres(c.genres);
    let genreMatch = 0;
    let bestGenreKey = null;
    for (const g of genres) {
      const gp = genrePref[g.trim()];
      if (gp) {
        const gScore = gp.count * gp.total;
        if (gScore > genreMatch) {
          genreMatch = gScore;
          bestGenreKey = g.trim();
        }
      }
    }
    if (genreMatch === 0) return null;
    const maxCount = Math.max(...Object.values(genrePref).map(g => g.count));
    const genreNorm = genrePref[bestGenreKey].count / maxCount;
    const alsScore = c.score ? Math.min(c.score / 5, 1) : 0.5;
    const avgScore = c.avg_rating ? Math.min(c.avg_rating / 5, 1) : 0.5;
    const finalScore = (genreNorm * 0.5 + avgScore * 0.3 + alsScore * 0.2) * 5;

    return {
      movie_id: c.movie_id,
      title: c.title,
      genres: c.genres,
      poster_url: c.poster_url,
      score: parseFloat(finalScore.toFixed(3)),
      bestGenre: bestGenreKey
    };
  }).filter(Boolean);

  const genreGroups = {};
  for (const m of scored) {
    if (!genreGroups[m.bestGenre]) genreGroups[m.bestGenre] = [];
    genreGroups[m.bestGenre].push(m);
  }
  for (const g of Object.keys(genreGroups)) {
    genreGroups[g].sort((a, b) => b.score - a.score);
  }
  const genreKeys = Object.keys(genreGroups);
  const result = [];
  while (result.length < 5) {
    let anyPicked = false;
    for (const g of genreKeys) {
      if (genreGroups[g].length > 0 && result.length < 5) {
        const pick = Math.floor(Math.random() * genreGroups[g].length);
        result.push(genreGroups[g].splice(pick, 1)[0]);
        anyPicked = true;
      }
    }
    if (!anyPicked) break;
  }
  return result;
}

async function genreWeightedRecommend(userId, limit = 20) {
  await pool.query('DELETE FROM rec_movies WHERE user_id = ?', [userId]);

  const [userRatings] = await pool.query(
    'SELECT r.rating, m.genres FROM ratings r JOIN movies m ON r.movie_id = m.movie_id WHERE r.user_id = ?',
    [userId]
  );

  const genreWeight = {};
  for (const r of userRatings) {
    splitGenres(r.genres).forEach(g => {
      const key = g.trim();
      if (!key) return;
      if (!genreWeight[key]) genreWeight[key] = { total: 0, count: 0 };
      genreWeight[key].total += r.rating;
      genreWeight[key].count += 1;
    });
  }
  for (const key of Object.keys(genreWeight)) {
    if (genreWeight[key].count < 2) delete genreWeight[key];
  }

  const [rated] = await pool.query('SELECT movie_id FROM ratings WHERE user_id = ?', [userId]);
  const ratedIds = rated.map(r => r.movie_id);
  const ratedSet = new Set(ratedIds);

  const [allMovies] = await pool.query('SELECT movie_id, title, genres, poster_url, avg_rating FROM movies');
  const candidates = [];

  for (const m of allMovies) {
    if (ratedSet.has(m.movie_id)) continue;
    const mGenres = splitGenres(m.genres);
    let matchScore = 0;
    let bestG = null;
    for (const g of mGenres) {
      const gw = genreWeight[g];
      if (gw) {
        const gScore = gw.count * gw.total;
        if (gScore > matchScore) {
          matchScore = gScore;
          bestG = g;
        }
      }
    }
    if (matchScore > 0) {
      const maxCount = Math.max(...Object.values(genreWeight).map(g => g.count));
      const normalizedMatch = genreWeight[bestG].count / maxCount;
      const avgRating = parseFloat(m.avg_rating || 4);
      const finalScore = normalizedMatch * 3 + Math.min(avgRating, 5) * 0.3;
      candidates.push({
        movie_id: m.movie_id,
        title: m.title,
        genres: m.genres,
        poster_url: m.poster_url,
        score: parseFloat(finalScore.toFixed(3)),
        bestGenre: bestG
      });
    }
  }

  const genreGroups = {};
  for (const c of candidates) {
    if (!genreGroups[c.bestGenre]) genreGroups[c.bestGenre] = [];
    genreGroups[c.bestGenre].push(c);
  }
  for (const g of Object.keys(genreGroups)) {
    genreGroups[g].sort((a, b) => b.score - a.score);
  }
  const genreKeys = Object.keys(genreGroups);
  const topN = [];
  while (topN.length < 20) {
    let anyPicked = false;
    for (const g of genreKeys) {
      if (genreGroups[g].length > 0 && topN.length < 20) {
        topN.push(genreGroups[g].shift());
        anyPicked = true;
      }
    }
    if (!anyPicked) break;
  }

  if (topN.length > 0) {
    const values = topN.map(c => [userId, c.movie_id, c.score]);
    await pool.query('INSERT INTO rec_movies (user_id, movie_id, score) VALUES ?', [values]);
  }

  if (topN.length === 0) {
    const [popular] = await pool.query(
      'SELECT movie_id, avg_rating FROM movies ORDER BY avg_rating DESC LIMIT 5'
    );
    const values = popular.map(p => [userId, p.movie_id, parseFloat(p.avg_rating || 4)]);
    await pool.query('INSERT INTO rec_movies (user_id, movie_id, score) VALUES ?', [values]);
  }
}

app.get('/api/recommend/reason', async (req, res) => {
  try {
    const userId = parseInt(req.query.user_id) || 1;
    const [ratings] = await pool.query(
      `SELECT r.rating, m.genres FROM ratings r JOIN movies m ON r.movie_id = m.movie_id
       WHERE r.user_id = ? AND r.rating >= 4`, [userId]
    );
    const tagStats = {};
    for (const r of ratings) {
      splitGenres(r.genres).forEach(g => {
        const key = g.trim();
        if (!key) return;
        if (!tagStats[key]) tagStats[key] = { total: 0, count: 0 };
        tagStats[key].total += r.rating;
        tagStats[key].count += 1;
      });
    }
    for (const key of Object.keys(tagStats)) {
      if (tagStats[key].count < 2) delete tagStats[key];
    }
    const sorted = Object.entries(tagStats)
      .sort((a, b) => {
        const sa = a[1].count * a[1].total;
        const sb = b[1].count * b[1].total;
        if (sb !== sa) return sb - sa;
        return b[1].total - a[1].total;
      })
      .slice(0, 3);
    const reasons = sorted.map(([name, stats]) => {
      const avg = (stats.total / stats.count).toFixed(1);
      return `你喜欢 ${name} 类型的电影，平均评分 ${avg} 分，看过 ${stats.count} 部`;
    });
    res.json({ reasons: reasons.length ? reasons : ['你还没有足够的评分记录，推荐热门佳片'] });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '获取推荐理由失败' });
  }
});

app.get('/api/user/preference', async (req, res) => {
  try {
    const userId = parseInt(req.query.user_id) || 1;
    const typeList = ['剧情','爱情','科幻','动画','喜剧','动作','奇幻','犯罪'];
    const [rows] = await pool.query(
      `SELECT m.genres, AVG(r.rating) as avg_rate
       FROM ratings r JOIN movies m ON r.movie_id = m.movie_id
       WHERE r.user_id = ? AND r.rating IS NOT NULL GROUP BY m.genres`, [userId]
    );
    const preference = {};
    typeList.forEach(t => { preference[t] = 0; });
    rows.forEach(row => {
      splitGenres(row.genres).forEach(g => {
        if (preference.hasOwnProperty(g)) preference[g] = Math.max(preference[g], parseFloat(row.avg_rate) || 0);
      });
    });
    res.json({ preference: typeList.map(t => ({ type: t, value: preference[t] || 0 })) });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '获取偏好失败' });
  }
});

app.get('/api/movie/detail', async (req, res) => {
  try {
    const movieId = parseInt(req.query.movie_id);
    if (isNaN(movieId)) return res.status(400).json({ error: '无效的电影ID' });
    const [[movie]] = await pool.query(
      'SELECT movie_id, title, genres, avg_rating, year, poster_url FROM movies WHERE movie_id = ?', [movieId]
    );
    if (!movie) return res.status(404).json({ error: '电影不存在' });
    const detail = movieDetails[movieId] || { description: '暂无简介', director: '未知' };
    res.json({
      title: movie.title || '未知电影', genres: movie.genres || '未知',
      avg_rating: movie.avg_rating || 0, year: movie.year || '未知',
      poster_url: movie.poster_url || '', description: detail.description,
      director: detail.director, movie_id: movie.movie_id
    });
  } catch (err) {
    console.error(err);
    res.status(200).json({ title: '加载失败', genres: '', avg_rating: 0, year: '', poster_url: '', description: '暂时无法获取电影信息，请稍后重试', director: '' });
  }
});

app.post('/api/rate', async (req, res) => {
  try {
    const { user_id, movie_id, rating } = req.body;
    await pool.query(
      'INSERT INTO ratings (user_id, movie_id, rating, timestamp) VALUES (?, ?, ?, UNIX_TIMESTAMP()) ON DUPLICATE KEY UPDATE rating = VALUES(rating), timestamp = UNIX_TIMESTAMP()',
      [user_id, movie_id, rating]
    );
    await pool.query(
      'UPDATE movies m SET avg_rating = (SELECT ROUND(AVG(rating), 1) FROM ratings WHERE movie_id = ?) WHERE m.movie_id = ?',
      [movie_id, movie_id]
    );
    delete sparkCache[user_id];
    console.log('[Rec] Cache cleared for user', user_id, 'after rating');
    res.json({ code: 0, msg: '评分成功' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '评分失败' });
  }
});

app.get('/api/user/ratings', async (req, res) => {
  try {
    const userId = parseInt(req.query.user_id) || 1;
    const [rows] = await pool.query(
      'SELECT r.movie_id, m.title, r.rating FROM ratings r JOIN movies m ON r.movie_id = m.movie_id WHERE r.user_id = ? ORDER BY r.timestamp DESC',
      [userId]
    );
    res.json({ ratings: rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '查询用户评分失败' });
  }
});

app.delete('/api/user/ratings', async (req, res) => {
  try {
    const { user_id, movie_id } = req.body;
    if (!user_id || !movie_id) return res.json({ code: 1, msg: '参数不完整' });
    await pool.query('DELETE FROM ratings WHERE user_id = ? AND movie_id = ?', [user_id, movie_id]);
    await pool.query(
      'UPDATE movies m SET avg_rating = (SELECT ROUND(AVG(rating), 1) FROM ratings WHERE movie_id = ?) WHERE m.movie_id = ?',
      [movie_id, movie_id]
    );
    res.json({ code: 0, msg: '已删除评分' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '删除失败' });
  }
});

app.post('/api/user/ratings/delete_batch', async (req, res) => {
  try {
    const { user_id, movie_ids } = req.body;
    if (!user_id || !movie_ids || !movie_ids.length) return res.json({ code: 1, msg: '参数不完整' });
    await pool.query('DELETE FROM ratings WHERE user_id = ? AND movie_id IN (?)', [user_id, movie_ids]);
    for (const mid of movie_ids) {
      await pool.query(
        'UPDATE movies m SET avg_rating = (SELECT ROUND(AVG(rating), 1) FROM ratings WHERE movie_id = ?) WHERE m.movie_id = ?',
        [mid, mid]
      );
    }
    res.json({ code: 0, msg: `已删除 ${movie_ids.length} 条评分` });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '删除失败' });
  }
});

app.get('/api/admin/users', async (req, res) => {
  try {
    const [users] = await pool.query(
      'SELECT user_id, username, is_admin, created_at FROM users ORDER BY user_id'
    );
    res.json({ users });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '查询用户失败' });
  }
});

app.delete('/api/admin/users/:id', async (req, res) => {
  try {
    const userId = parseInt(req.params.id);
    await pool.query('DELETE FROM ratings WHERE user_id = ?', [userId]);
    await pool.query('DELETE FROM rec_movies WHERE user_id = ?', [userId]);
    await pool.query('DELETE FROM users WHERE user_id = ?', [userId]);
    res.json({ code: 0, msg: '已删除用户及关联数据' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '删除失败' });
  }
});

app.get('/api/admin/movies', async (req, res) => {
  try {
    const [movies] = await pool.query(
      'SELECT movie_id, title, genres, avg_rating, year FROM movies ORDER BY movie_id'
    );
    res.json({ movies });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '查询电影失败' });
  }
});

app.delete('/api/admin/movies/:id', async (req, res) => {
  try {
    const movieId = parseInt(req.params.id);
    await pool.query('DELETE FROM ratings WHERE movie_id = ?', [movieId]);
    await pool.query('DELETE FROM rec_movies WHERE movie_id = ?', [movieId]);
    await pool.query('DELETE FROM movies WHERE movie_id = ?', [movieId]);
    res.json({ code: 0, msg: '已删除电影及关联数据' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '删除失败' });
  }
});

app.post('/api/admin/movies', async (req, res) => {
  try {
    const { title, genres, year, poster_url } = req.body;
    if (!title) return res.json({ code: 1, msg: '电影名不能为空' });
    const [result] = await pool.query(
      'INSERT INTO movies (title, genres, year, poster_url) VALUES (?, ?, ?, ?)',
      [title, genres || '', year || new Date().getFullYear(), poster_url || '']
    );
    res.json({ code: 0, msg: '添加成功', movie_id: result.insertId });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '添加失败' });
  }
});

async function initializeAdmin() {
  try {
    const md5 = crypto.createHash('md5').update(ADMIN_PASSWORD).digest('hex');
    const [existingAdmin] = await pool.query(
      'SELECT user_id FROM users WHERE username = ? AND is_admin = 1',
      ['admin']
    );
    
    if (existingAdmin.length === 0) {
      await pool.query(
        'INSERT INTO users (username, password, is_admin) VALUES (?, ?, 1)',
        ['admin', md5]
      );
    }
  } catch (err) {
    console.error('Failed to create admin account:', err.message);
  }
}

initializeAdmin();

app.listen(PORT, () => {
  console.log(`FilmRadar Server running at http://0.0.0.0:${PORT}`);
  console.log(`Spark Submit: ${SPARK_SUBMIT}`);
  console.log(`ALS Script: ${ALS_SCRIPT}`);
});
