/* ============================================================
   GeoGebra Multiplayer Server
   Локальная сеть. Запуск: node server.js
   ============================================================ */
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const os = require('os');

const PORT = 3000;
const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

// Раздаём index.html и статику из папки проекта
app.use(express.static(__dirname));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'GeoTrainForBrainv1.7.html')));

/* ============================================================
   СОСТОЯНИЕ
============================================================ */
const waitingQueue = [];     // игроки, ждущие соперника
const rooms = new Map();     // roomId -> room
const MAX_SPECTATORS = 5;
const ROUND_TIMEOUT_MS = 90000;
const SKIP_REQUEST_TIMEOUT_MS = 20000;
const MAX_ROUNDS = 5;

/* ============================================================
   УТИЛИТЫ
============================================================ */
function generateRoomId() {
    const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
    let id = '';
    for (let i = 0; i < 4; i++) id += chars[Math.floor(Math.random() * chars.length)];
    return rooms.has(id) ? generateRoomId() : id;
}
function generateMatchId() {
    return 'match_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
}

/* ============================================================
   СОКЕТЫ
============================================================ */
io.on('connection', socket => {
    console.log('🔌 Подключение:', socket.id);

    /* --- Регистрация игрока в очередь --- */
    socket.on('find_match', ({ nickname }) => {
        socket.nickname = (nickname || 'Игрок').slice(0, 20);
        socket.role = 'player';
        
        // Проверяем очередь на живого соперника
        const opponent = waitingQueue.find(s => s.connected && s.id !== socket.id);
        
        if (opponent) {
            // Убираем из очереди
            const idx = waitingQueue.indexOf(opponent);
            if (idx >= 0) waitingQueue.splice(idx, 1);
            createRoom(opponent, socket);
        } else {
            waitingQueue.push(socket);
            socket.emit('waiting_for_opponent');
        }
    });

    /* --- Отмена поиска --- */
    socket.on('cancel_search', () => {
        const idx = waitingQueue.indexOf(socket);
        if (idx >= 0) waitingQueue.splice(idx, 1);
        socket.emit('search_cancelled');
    });

    /* --- Создание приватной комнаты --- */
    socket.on('create_private_room', ({ nickname }) => {
        socket.nickname = (nickname || 'Игрок').slice(0, 20);
        socket.role = 'player';
        const roomId = generateRoomId();
        const room = createEmptyRoom(roomId);
        room.players.push(socket);
        socket.join(roomId);
        socket.roomId = roomId;
        rooms.set(roomId, room);
        socket.emit('room_created', { roomId });
    });

    /* --- Присоединение к приватной комнате --- */
    socket.on('join_private_room', ({ roomId, nickname }) => {
        const room = rooms.get(roomId.toUpperCase());
        if (!room) { socket.emit('join_error', { msg: 'Комната не найдена' }); return; }
        if (room.players.length >= 2) { socket.emit('join_error', { msg: 'Комната заполнена' }); return; }
        
        socket.nickname = (nickname || 'Игрок').slice(0, 20);
        socket.role = 'player';
        room.players.push(socket);
        socket.join(roomId);
        socket.roomId = roomId;
        
        startMatch(room);
    });

    /* --- Запрос на смену формулы сопернику (в реальном времени) --- */
    socket.on('formula_update', ({ formula }) => {
        const room = rooms.get(socket.roomId);
        if (!room) return;
        socket.formula = formula;
        // Рассылаем всем кроме себя
        socket.to(socket.roomId).emit('opponent_formula', {
            playerId: socket.id,
            nickname: socket.nickname,
            formula: formula
        });
    });

    /* --- Игрок попал в цель --- */
    socket.on('player_hit', ({ timeMs }) => {
        const room = rooms.get(socket.roomId);
        if (!room || !room.currentRound) return;
        const round = room.currentRound;
        
        // Если раунд уже завершён или игрок уже попал — игнорируем
        if (round.finished) return;
        if (round.hits[socket.id]) return;
        
        round.hits[socket.id] = { timeMs, at: Date.now() };
        
        io.to(socket.roomId).emit('player_hit_announce', {
            playerId: socket.id,
            nickname: socket.nickname,
            timeMs
        });
        
        // Проверяем, все ли попали (или кто-то сдался)
        checkRoundComplete(room);
    });

    /* --- Игрок нажал «Пропустить» --- */
    socket.on('request_skip', () => {
        const room = rooms.get(socket.roomId);
        if (!room || !room.currentRound) return;
        const opponent = room.players.find(p => p.id !== socket.id);
        if (!opponent) return;
        
        // Проверка лимита пропусков
        if (room.skipsUsed[socket.id]) {
            socket.emit('skip_error', { msg: 'Лимит пропусков исчерпан' });
            return;
        }
        
        room.currentRound.pendingSkip = {
            from: socket.id,
            fromNickname: socket.nickname,
            requestedAt: Date.now()
        };
        
        opponent.emit('skip_request', {
            fromNickname: socket.nickname,
            timeoutSec: SKIP_REQUEST_TIMEOUT_MS / 1000
        });
        
        // Таймаут — 20 секунд, авто-отказ
        if (room.currentRound.skipTimer) clearTimeout(room.currentRound.skipTimer);
        room.currentRound.skipTimer = setTimeout(() => {
            if (room.currentRound && room.currentRound.pendingSkip) {
                handleSkipResponse(room, socket.id, false, true);
            }
        }, SKIP_REQUEST_TIMEOUT_MS);
    });

    /* --- Ответ на запрос пропуска --- */
    socket.on('skip_response', ({ accepted }) => {
        const room = rooms.get(socket.roomId);
        if (!room || !room.currentRound || !room.currentRound.pendingSkip) return;
        handleSkipResponse(room, room.currentRound.pendingSkip.from, accepted, false);
    });

    /* --- Игрок сдался в матче --- */
    socket.on('forfeit_match', () => {
        const room = rooms.get(socket.roomId);
        if (!room) return;
        const opponent = room.players.find(p => p.id !== socket.id);
        if (opponent) {
            io.to(socket.roomId).emit('match_finished', {
                reason: 'forfeit',
                winnerId: opponent.id,
                forfeitBy: socket.id
            });
        }
        room.players.forEach(p => { p.roomId = null; p.leave(socket.roomId); });
        rooms.delete(socket.roomId);
    });

    /* --- Наблюдатель подключается к комнате --- */
    socket.on('spectate', ({ roomId }) => {
        const room = rooms.get(roomId);
        if (!room) { socket.emit('spectate_error', { msg: 'Матч не найден' }); return; }
        if (room.spectators.length >= MAX_SPECTATORS) {
            socket.emit('spectate_error', { msg: 'Слишком много зрителей' });
            return;
        }
        
        socket.role = 'spectator';
        socket.roomId = roomId;
        room.spectators.push(socket);
        socket.join(roomId);
        
        // Отправляем текущее состояние
        socket.emit('spectate_state', {
            roomId,
            players: room.players.map(p => ({
                id: p.id, nickname: p.nickname, formula: p.formula || '',
                skin: p.skin || null
            })),
            round: room.currentRound ? room.currentRound.number : 0,
            level: room.currentRound ? room.currentRound.level : null,
            scores: room.scores
        });
        
        io.to(roomId).emit('spectator_joined', {
            count: room.spectators.length
        });
    });

    /* --- Наблюдатель уходит --- */
    socket.on('stop_spectating', () => {
        const room = rooms.get(socket.roomId);
        if (room) {
            room.spectators = room.spectators.filter(s => s.id !== socket.id);
            socket.leave(socket.roomId);
            io.to(socket.roomId).emit('spectator_left', { count: room.spectators.length });
        }
        socket.roomId = null;
        socket.role = null;
    });

    /* --- Запрос списка активных матчей --- */
    socket.on('get_active_matches', () => {
        const list = [];
        rooms.forEach((room, id) => {
            if (room.players.length === 2 && room.currentRound && !room.currentRound.finished) {
                list.push({
                    roomId: id,
                    players: room.players.map(p => p.nickname),
                    round: room.currentRound.number,
                    totalRounds: 3,
                    level: room.currentRound.level ? room.currentRound.level.name : '?',
                    spectators: room.spectators.length,
                    startedAt: room.startedAt
                });
            }
        });
        socket.emit('active_matches', list);
    });

    /* --- Игрок передаёт свой скин при подключении --- */
    socket.on('set_skin', ({ skin }) => {
        socket.skin = skin;
        const room = rooms.get(socket.roomId);
        if (room) {
            const opponent = room.players.find(p => p.id !== socket.id);
            if (opponent) opponent.emit('opponent_skin', { playerId: socket.id, skin });
        }
    });

    /* --- Отключение --- */
    socket.on('disconnect', () => {
        console.log('❌ Отключение:', socket.id);
        const idx = waitingQueue.indexOf(socket);
        if (idx >= 0) waitingQueue.splice(idx, 1);
        
        const room = rooms.get(socket.roomId);
        if (room) {
            if (socket.role === 'player') {
                const opponent = room.players.find(p => p.id !== socket.id && p.connected);
                if (opponent) {
                    io.to(socket.roomId).emit('match_finished', {
                        reason: 'disconnect',
                        winnerId: opponent.id,
                        forfeitBy: socket.id
                    });
                }
                room.players.forEach(p => { p.roomId = null; p.leave(socket.roomId); });
                rooms.delete(socket.roomId);
            } else {
                room.spectators = room.spectators.filter(s => s.id !== socket.id);
                io.to(socket.roomId).emit('spectator_left', { count: room.spectators.length });
            }
        }
    });
});

/* ============================================================
   ЛОГИКА КОМНАТ
============================================================ */
function createEmptyRoom(roomId) {
    return {
        roomId,
        players: [],
        spectators: [],
        scores: {},          // playerId -> кол-во выигранных раундов
        totalTimes: {},      // playerId -> сумма времени в мс
        skipsUsed: {},       // playerId -> true/false
        roundHistory: [],    // [{ number, level, hits: { id: {timeMs} }, winnerId }]
        currentRound: null,
        matchId: generateMatchId(),
        startedAt: Date.now(),
        roundsPlayed: 0
    };
}

function createRoom(p1, p2) {
    const roomId = generateRoomId();
    const room = createEmptyRoom(roomId);
    room.players = [p1, p2];
    p1.join(roomId); p1.roomId = roomId;
    p2.join(roomId); p2.roomId = roomId;
    room.scores[p1.id] = 0;
    room.scores[p2.id] = 0;
    room.totalTimes[p1.id] = 0;
    room.totalTimes[p2.id] = 0;
    room.skipsUsed[p1.id] = false;
    room.skipsUsed[p2.id] = false;
    rooms.set(roomId, room);
    startMatch(room);
}

/* Сервер сам выбирает уровни. Клиент шлёт свои levelPools при find_match — 
   здесь заглушка, клиент подставит свой выбор. Для простоты — 
   фиксированный список ID, клиент знает эти ID. */
function pickLevelsForMatch() {
    // Клиент должен знать эти же ID
    const easy = ['MP-L1', 'MP-L2', 'MP-L3', 'MP-L4', 'MP-L5', 'MP-L6', 'MP-L7'];
    const medium = ['MP-M1', 'MP-M2', 'MP-M3', 'MP-M4', 'MP-M5', 'MP-M6', 'MP-M7'];
    const hard = ['MP-H1', 'MP-H2', 'MP-H3', 'MP-H4', 'MP-H5', 'MP-H6'];
    const pick = arr => arr[Math.floor(Math.random() * arr.length)];
    return [pick(easy), pick(medium), pick(hard)];
}

function startMatch(room) {
    const levels = pickLevelsForMatch();
    io.to(room.roomId).emit('match_start', {
        matchId: room.matchId,
        levels,
        players: room.players.map(p => ({
            id: p.id,
            nickname: p.nickname,
            skin: p.skin || null
        }))
    });
    setTimeout(() => startRound(room, 0, levels), 500);
}

function startRound(room, index, levels) {
    if (index >= 3 && room.players.every(p => room.totalTimes[p.id] !== room.totalTimes[room.players[0].id])) {
        // После 3 раундов — если ничья, играем 4-й и 5-й
    }
    if (index >= MAX_ROUNDS) { finishMatch(room, 'max_rounds'); return; }
    
    const levelId = levels[index] || levels[Math.floor(Math.random() * levels.length)];
    
    room.currentRound = {
        number: index + 1,
        levelId: levelId,
        level: null, // клиент сам восстановит по ID
        hits: {},     // playerId -> { timeMs, at }
        finished: false,
        startedAt: Date.now(),
        pendingSkip: null,
        skipTimer: null
    };
    room.roundsPlayed = index + 1;
    
    io.to(room.roomId).emit('round_start', {
        roundNumber: index + 1,
        levelId: levelId,
        isExtraRound: index >= 3
    });
    
    // Таймаут раунда — 90 секунд
    room.currentRound.timeoutTimer = setTimeout(() => {
        if (room.currentRound && !room.currentRound.finished) {
            endRound(room, 'timeout');
        }
    }, ROUND_TIMEOUT_MS);
}

function checkRoundComplete(room) {
    const round = room.currentRound;
    if (!round || round.finished) return;
    const bothHit = room.players.every(p => round.hits[p.id]);
    if (bothHit) endRound(room, 'both_hit');
}

function endRound(room, reason) {
    const round = room.currentRound;
    if (!round || round.finished) return;
    round.finished = true;
    clearTimeout(round.timeoutTimer);
    if (round.skipTimer) clearTimeout(round.skipTimer);
    
    // Записываем времена
    room.players.forEach(p => {
        const hit = round.hits[p.id];
        const time = hit ? hit.timeMs : ROUND_TIMEOUT_MS;
        room.totalTimes[p.id] = (room.totalTimes[p.id] || 0) + time;
    });
    
    // Определяем победителя раунда
    let winnerId = null;
    if (reason === 'both_hit') {
        const times = room.players.map(p => ({ id: p.id, t: round.hits[p.id].timeMs }));
        times.sort((a, b) => a.t - b.t);
        if (times[0].t === times[1].t) {
            // Ничья в раунде
            winnerId = null;
        } else {
            winnerId = times[0].id;
            room.scores[winnerId]++;
        }
    } else if (reason === 'timeout') {
        // Оба не успели — ничья
        winnerId = null;
    }
    
    room.roundHistory.push({
        number: round.number,
        levelId: round.levelId,
        hits: { ...round.hits },
        winnerId,
        reason
    });
    
    io.to(room.roomId).emit('round_end', {
        roundNumber: round.number,
        reason,
        hits: Object.keys(round.hits).reduce((acc, id) => {
            acc[id] = round.hits[id].timeMs;
            return acc;
        }, {}),
        winnerId,
        scores: room.scores,
        totalTimes: room.totalTimes
    });
    
    // Проверяем условие ничьей после 3 раундов
    const allTimesEqual = room.players[0] && room.players[1] &&
        Math.abs(room.totalTimes[room.players[0].id] - room.totalTimes[room.players[1].id]) < 100;
    
    let nextIndex = round.number; // 0-based: следующий раунд = number (т.к. number = index+1)
    
    if (round.number >= 3 && allTimesEqual) {
        // Ничья после 3 — ещё раунд
        if (round.number >= MAX_ROUNDS) {
            // Больше нельзя — принудительная ничья
            setTimeout(() => finishMatch(room, 'draw_max'), 1000);
            return;
        }
        // Продолжаем
    }
    
    if (round.number < 3) {
        setTimeout(() => startRound(room, round.number, pickLevelsForMatch()), 2000);
    } else if (allTimesEqual && round.number < MAX_ROUNDS) {
        setTimeout(() => startRound(room, round.number, pickLevelsForMatch()), 2000);
    } else {
        setTimeout(() => finishMatch(room, 'normal'), 1000);
    }
}

function handleSkipResponse(room, fromPlayerId, accepted, isTimeout) {
    const round = room.currentRound;
    if (!round || !round.pendingSkip) return;
    if (round.skipTimer) clearTimeout(round.skipTimer);
    
    const fromPlayer = room.players.find(p => p.id === fromPlayerId);
    
    if (accepted) {
        room.skipsUsed[fromPlayerId] = true;
        round.pendingSkip = null;
        
        io.to(room.roomId).emit('skip_accepted', {
            byNickname: fromPlayer ? fromPlayer.nickname : '?'
        });
        
        // Раунд заканчивается без штрафа — как ничейный
        if (!round.finished) {
            round.finished = true;
            clearTimeout(round.timeoutTimer);
            room.roundHistory.push({
                number: round.number,
                levelId: round.levelId,
                hits: { ...round.hits },
                winnerId: null,
                reason: 'skip'
            });
            
            io.to(room.roomId).emit('round_end', {
                roundNumber: round.number,
                reason: 'skip',
                hits: {},
                winnerId: null,
                scores: room.scores,
                totalTimes: room.totalTimes
            });
            
            // Ничья в раунде не влияет на общий счёт времени
            const roundNum = round.number;
            if (roundNum < 3) {
                setTimeout(() => startRound(room, roundNum, pickLevelsForMatch()), 2000);
            } else {
                setTimeout(() => finishMatch(room, 'normal'), 1000);
            }
        }
    } else {
        round.pendingSkip = null;
        io.to(room.roomId).emit('skip_denied', {
            byNickname: room.players.find(p => p.id !== fromPlayerId)?.nickname || '?',
            isTimeout
        });
    }
}

function finishMatch(room, reason) {
    if (room.finished) return;
    room.finished = true;
    
    const [p1, p2] = room.players;
    if (!p1 || !p2) return;
    
    const t1 = room.totalTimes[p1.id] || 0;
    const t2 = room.totalTimes[p2.id] || 0;
    
    let winnerId = null;
    let isDraw = false;
    
    if (Math.abs(t1 - t2) < 100 && reason === 'draw_max') {
        isDraw = true;
    } else if (t1 < t2) {
        winnerId = p1.id;
    } else if (t2 < t1) {
        winnerId = p2.id;
    } else {
        isDraw = true;
    }
    
    io.to(room.roomId).emit('match_finished', {
        reason,
        winnerId,
        isDraw,
        players: room.players.map(p => ({
            id: p.id,
            nickname: p.nickname,
            totalTime: room.totalTimes[p.id],
            roundsWon: room.scores[p.id]
        })),
        roundHistory: room.roundHistory
    });
    
    // Убираем комнату через 10 секунд
    setTimeout(() => {
        rooms.delete(room.roomId);
    }, 10000);
}

/* ============================================================
   ЗАПУСК
============================================================ */
server.listen(PORT, '0.0.0.0', () => {
    const nets = os.networkInterfaces();
    const ips = [];
    for (const name of Object.keys(nets)) {
        for (const net of nets[name]) {
            if (net.family === 'IPv4' && !net.internal) ips.push(net.address);
        }
    }
    console.log('\n🎮 GeoGebra Multiplayer Server запущен!\n');
    console.log(`   Локально:      http://localhost:${PORT}`);
    ips.forEach(ip => console.log(`   Для друзей:    http://${ip}:${PORT}`));
    console.log('\n   Открой игру в браузере и играй по локальной сети.\n');
});