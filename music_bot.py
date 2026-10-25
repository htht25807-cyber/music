import asyncio
import os
import random
import shutil
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import discord
from discord import app_commands
import yt_dlp
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import aiohttp
from aiohttp import web

# ==================== CONFIG ====================

TOKEN = os.environ.get("DISCORD_TOKEN")

try:
    sp = spotipy.Spotify(
        auth_manager=SpotifyClientCredentials(
            client_id=os.environ["SPOTIFY_ID"],
            client_secret=os.environ["SPOTIFY_SECRET"],
        )
    )
except Exception:
    sp = None

YDL_OPTS = {
    "format": "bestaudio[acodec=opus]/bestaudio/best",
    "noplaylist": True,
    "quiet": True,
    "no_warnings": True,
    "socket_timeout": 10,
    "retries": 2,
    "cachedir": "/tmp/yt-dlp-cache",
    "check_formats": False,

    "extractor_args": {
        "youtube": {
            "skip": ["dash", "hls", "translated_subs"]
        },
        "youtubetab": {
            "skip": ["authcheck"]
        }
    },
}

FFMPEG_OPTS = {
    "before_options": (
        "-nostdin "
        "-reconnect 1 "
        "-reconnect_streamed 1 "
        "-reconnect_delay_max 5 "
        "-analyzeduration 0 "
        "-probesize 32768"
    ),
    "options": "-vn",
}
# Cookies YouTube (không bắt buộc): trên Render thêm Secret File tên cookies.txt
COOKIES_SRC = os.environ.get("YT_COOKIES_FILE", "/etc/secrets/cookies.txt")
COOKIE_OPTS = {}
if os.path.exists(COOKIES_SRC):
    try:
        # Chép sang /tmp vì yt-dlp cần ghi lại cookies, còn /etc/secrets chỉ đọc được
        shutil.copyfile(COOKIES_SRC, "/tmp/yt_cookies.txt")
        COOKIE_OPTS = {"cookiefile": "/tmp/yt_cookies.txt"}
        print("Đã nạp cookies YouTube.")
    except Exception as exc:
        print("Không nạp được cookies YouTube:", exc)
else:
    print("Chưa có cookies YouTube (YouTube có thể chặn IP của server).")
YDL_OPTS.update(COOKIE_OPTS)

# Bài tự động phát tiếp sẽ bỏ qua video dài hơn mức này (livestream, mix hàng giờ...)
AUTO_MAX_SECONDS = 20 * 60

# Tự dọn tin nhắn cũ của bot trong các kênh đã dùng lệnh
CLEANUP_INTERVAL = 300       # Quét mỗi 5 phút
CLEANUP_MIN_AGE = 300        # Chỉ xóa tin nhắn của bot cũ hơn 5 phút
CLEANUP_IDLE_LIMIT = 3600    # Kênh 1 giờ không có lệnh thì dọn lần cuối rồi thôi theo dõi

# Lấy tên bài YouTube nhanh (không cần yt-dlp) để hiện hàng đợi
OEMBED_URL = "https://www.youtube.com/oembed"

# Pool luồng riêng + cache kết quả tìm kiếm (link stream YouTube sống ~6 giờ)
EXECUTOR = ThreadPoolExecutor(max_workers=4)
CACHE_TTL = 4 * 3600
_cache = {}                  # query -> (hết hạn, data)
_inflight = {}               # query -> Future (gộp các yêu cầu trùng nhau)
related_tasks = {}           # guild_id -> Future tìm sẵn bài gợi ý
_http_session = None         # aiohttp session dùng chung (oEmbed)

# Dữ liệu riêng cho từng server
queues = defaultdict(list)                  # Hàng đợi phát lần lượt
volumes = defaultdict(lambda: 1.0)           # 1.0 = 100%
music_lists = defaultdict(list)              # Danh sách nhạc người dùng thêm
autoplay = defaultdict(lambda: True)         # Tự phát tiếp khi hàng đợi trống
history = defaultdict(list)                  # Lịch sử bài đã phát
current_song = {}                            # Query bài đang phát
play_channels = {}                           # Kênh nhắn thông báo phát nhạc
return_pending = set()                       # Tránh lặp thao tác /return
now_data = {}                                # Thông tin bài đang/vừa phát (tên, link, id YouTube...)
now_playing_msg = {}                         # Tin nhắn menu điều khiển đang hiện
recent_ids = defaultdict(list)               # id video YouTube đã phát (để không phát lặp)
titles = {}                                  # query/link -> tên bài (để hiển thị đẹp hơn)
stopping = set()                             # Server đang chạy /stop (chặn tự phát bài mới)
auto_paused = set()                          # Server đang tạm dừng vì kênh thoại trống (tự tiếp tục khi có người về)
waiting_for_listener = set()                 # Hết bài lúc kênh trống: chờ có người vào lại mới phát tiếp
play_gen = defaultdict(int)                  # Đánh số mỗi lần phát; callback của lần phát cũ (bị tua/thay) sẽ bị bỏ qua
cleanup_channels = {}                        # channel_id -> [kênh, lần cuối có người dùng lệnh]

# Dữ liệu riêng cho từng người dùng
favorites = defaultdict(list)                # user_id -> [{"title": ..., "url": ...}]


# ==================== SEARCH MUSIC ====================

def extract(query: str) -> dict:
    """Lấy URL stream và tên bài hát bằng yt-dlp (đồng bộ, chạy trong luồng riêng)."""
    if query.startswith("http"):
        candidates = [query]
    else:
        candidates = [f"ytsearch1:{query}", f"scsearch1:{query}"]

    last_err = None
    with yt_dlp.YoutubeDL(YDL_OPTS) as ydl:  # Tạo 1 lần cho cả vòng lặp
        for candidate in candidates:
            try:
                info = ydl.extract_info(candidate, download=False)

                if not info:
                    continue

                if "entries" in info:
                    entries = info.get("entries") or []
                    if not entries:
                        continue
                    info = entries[0]

                if info and info.get("url"):
                    return {
                        "url": info["url"],
                        "title": info.get("title", query),
                        "page": info.get("webpage_url")
                        or (candidate if candidate.startswith("http") else None),
                        "yt_id": info.get("id")
                        if info.get("extractor_key") == "Youtube" else None,
                        "duration": info.get("duration"),
                        "uploader": info.get("uploader") or info.get("channel"),
                        "thumbnail": info.get("thumbnail"),
                        "chapters": info.get("chapters"),
                    }
            except Exception as exc:
                last_err = exc

    if last_err:
        raise last_err
    raise RuntimeError("Không tìm thấy bài hát.")


async def extract_async(query: str) -> dict:
    """Không chặn event loop, có cache, gộp các yêu cầu trùng nhau."""
    key = query.strip().lower()
    hit = _cache.get(key)
    if hit and hit[0] > time.time():
        return dict(hit[1])

    if key in _inflight:
        return dict(await asyncio.shield(_inflight[key]))

    loop = asyncio.get_running_loop()
    fut = loop.run_in_executor(EXECUTOR, extract, query)
    _inflight[key] = fut
    try:
        data = await fut
    finally:
        _inflight.pop(key, None)

    _cache[key] = (time.time() + CACHE_TTL, data)
    if len(_cache) > 300:  # Giữ RAM nhỏ trên Render
        for k in sorted(_cache, key=lambda k: _cache[k][0])[:100]:
            _cache.pop(k, None)
    return dict(data)


def prefetch_next(gid: int):
    """Tìm sẵn bài kế tiếp trong hàng đợi để chuyển bài gần như tức thì."""
    q = queues[gid]
    if q and isinstance(q[0], str):
        asyncio.ensure_future(_safe_prefetch(q[0]))


async def _safe_prefetch(query: str):
    try:
        await extract_async(query)
    except Exception:
        pass


# ==================== SPOTIFY ====================

def spotify_to_queries(link: str) -> list[str]:
    if sp is None:
        raise RuntimeError("Chưa cấu hình SPOTIFY_ID và SPOTIFY_SECRET.")

    if "/track/" in link:
        track = sp.track(link)
        return [f"{track['artists'][0]['name']} - {track['name']}"]

    if "/album/" in link:
        items = sp.album_tracks(link, limit=50)["items"]
        return [
            f"{item['artists'][0]['name']} - {item['name']}"
            for item in items if item.get("artists")
        ]

    if "/playlist/" in link:
        result = []
        results = sp.playlist_items(link, limit=100)
        for item in results.get("items", []):
            track = item.get("track")
            if track and track.get("artists"):
                result.append(
                    f"{track['artists'][0]['name']} - {track['name']}"
                )
        return result

    raise RuntimeError(
        "Link Spotify không được hỗ trợ. Dùng link track, album hoặc playlist."
    )


# ==================== AUTO NEXT (BÀI GỢI Ý) ====================

def find_related(video_id, title, played: set):
    """
    Tìm bài hát liên quan bằng YouTube Search, không dùng YouTube Mix.
    Bỏ qua bài đã phát, bài hiện tại và video quá dài.
    """
    if not title or not title.strip():
        return None

    opts = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": "in_playlist",
        "playlistend": 10,
        "socket_timeout": 10,
        "retries": 2,
        "cachedir": "/tmp/yt-dlp-cache",
        **COOKIE_OPTS,
    }

    # Chỉ tìm bằng tên bài hát, không truy cập playlist RD...
    query = f"ytsearch10:{title.strip()}"

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(query, download=False)
    except Exception as exc:
        print("Không tìm được bài gợi ý:", exc)
        return None

    entries = [
        entry
        for entry in (info or {}).get("entries") or []
        if entry
    ]

    # Không phát lại bài hiện tại hoặc những bài đã phát gần đây.
    excluded_ids = set(played or set())

    if video_id:
        excluded_ids.add(video_id)

    for entry in entries:
        vid = entry.get("id")

        if not vid or vid in excluded_ids:
            continue

        # Bỏ qua livestream và video đang phát trực tiếp.
        if entry.get("is_live"):
            continue

        if entry.get("live_status") in ("is_live", "is_upcoming"):
            continue

        # Bỏ qua video dài hơn giới hạn autoplay.
        duration = entry.get("duration")

        if duration is not None and duration > AUTO_MAX_SECONDS:
            continue

        return {
            "id": vid,
            "url": f"https://www.youtube.com/watch?v={vid}",
        }

    return None


def prefetch_related(gid: int):
    """
    Tìm trước bài liên quan trong lúc bài hiện tại đang phát.
    Chỉ tìm khi hàng đợi và danh sách nhạc đều trống.
    """
    if not autoplay[gid] or queues[gid] or music_lists[gid]:
        return

    data = now_data.get(gid) or {}
    title = data.get("title")

    if not title:
        return

    loop = asyncio.get_running_loop()

    # Hủy tham chiếu tác vụ cũ để tránh giữ task không cần thiết.
    old_task = related_tasks.get(gid)

    if old_task is not None and not old_task.done():
        return

    related_tasks[gid] = loop.run_in_executor(
        EXECUTOR,
        find_related,
        data.get("yt_id"),
        title,
        set(recent_ids[gid]),
    )


async def get_related_async(gid: int):
    """
    Lấy bài đã tìm trước; nếu chưa có thì tìm ngay
    mà không chặn Discord event loop.
    """
    task = related_tasks.pop(gid, None)

    if task is not None:
        try:
            result = await task

            # Kiểm tra lại để tránh phát trùng bài.
            if result and result["id"] not in recent_ids[gid]:
                return result

        except Exception as exc:
            print("Lỗi lấy bài gợi ý đã tìm trước:", exc)

    seed = now_data.get(gid) or {}
    title = seed.get("title")

    if not title:
        return None

    loop = asyncio.get_running_loop()

    return await loop.run_in_executor(
        EXECUTOR,
        find_related,
        seed.get("yt_id"),
        title,
        set(recent_ids[gid]),
    )

# ==================== PHÁT NHẠC & TUA ====================

def fmt_pos(sec) -> str:
    """Vị trí trong bài, luôn hiện số (0 giây -> 0:00)."""
    sec = max(0, int(sec or 0))
    h, rest = divmod(sec, 3600)
    m, s = divmod(rest, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


class TrackedSource(discord.PCMVolumeTransformer):
    """Nguồn phát có đếm vị trí hiện tại (mỗi lần đọc = 20ms âm thanh)."""

    def __init__(self, original, volume: float = 1.0, start_at: float = 0.0):
        super().__init__(original, volume=volume)
        self.start_at = start_at
        self.frames = 0

    def read(self):
        data = super().read()
        if data:
            self.frames += 1
        return data

    @property
    def position(self) -> float:
        return self.start_at + self.frames * 0.02


def start_playback(guild: discord.Guild, channel, data: dict, start_at: float = 0.0):
    """Bắt đầu phát bài (từ giây start_at). Hết bài thì tự gọi play_next."""
    gid = guild.id
    vc = guild.voice_client
    play_gen[gid] += 1
    gen = play_gen[gid]

    def after(error):
        if error:
            print("Lỗi phát nhạc:", error)
        if play_gen[gid] != gen:
            return  # Lần phát này đã bị tua/thay bằng lần khác, đừng chuyển bài
        asyncio.run_coroutine_threadsafe(play_next(guild, channel), bot.loop)

    before = FFMPEG_OPTS["before_options"]
    if start_at > 0:
        before = f"-ss {start_at:.1f} " + before
    audio = discord.FFmpegPCMAudio(
        data["url"], before_options=before, options=FFMPEG_OPTS["options"]
    )
    source = TrackedSource(audio, volume=volumes[gid], start_at=start_at)
    vc.play(source, after=after)

    # Tìm sẵn bài kế tiếp (không làm lại mỗi lần tua)
    if start_at == 0:
        try:
            prefetch_next(gid)
            prefetch_related(gid)
        except Exception as exc:
            print("Lỗi prefetch:", exc)


def seek_to(guild: discord.Guild, seconds: float) -> bool:
    """Tua bài đang phát đến giây `seconds` (giữ nguyên trạng thái tạm dừng)."""
    gid = guild.id
    vc = guild.voice_client
    data = now_data.get(gid)
    if not vc or not data or not (vc.is_playing() or vc.is_paused()):
        return False

    was_paused = vc.is_paused()
    play_gen[gid] += 1  # Vô hiệu hóa callback của lần phát cũ
    vc.stop()
    start_playback(guild, play_channels.get(gid), data, start_at=seconds)
    if was_paused:
        vc.pause()
    return True


def seek_points(data: dict) -> list:
    """Các điểm có thể tua tới: chương của video nếu có, không thì chia đều 10 đoạn."""
    chapters = data.get("chapters") or []
    if chapters:
        return [
            (c.get("title") or "Đoạn", int(c.get("start_time") or 0))
            for c in chapters[:25]
        ]
    duration = data.get("duration")
    if duration and duration >= 30:
        return [(f"{p}% bài", int(duration * p / 100)) for p in range(0, 100, 10)]
    return []


# ==================== NOW PLAYING MENU ====================

async def clear_menu(gid: int):
    """Xóa menu cũ (không còn dùng được) khỏi kênh chat."""
    msg = now_playing_msg.pop(gid, None)
    if msg:
        try:
            await msg.delete()
        except Exception:
            pass


def build_now_playing_embed(data: dict, gid: int, auto: bool = False) -> discord.Embed:
    embed = discord.Embed(
        title=data["title"][:250],
        url=data.get("page"),
        color=discord.Color.blurple(),
    )
    embed.add_field(name="⏱️ Thời lượng", value=fmt_duration(data.get("duration")))
    embed.add_field(name="👤 Kênh", value=(data.get("uploader") or "?")[:100])
    embed.add_field(name="🔊 Âm lượng", value=f"{int(round(volumes[gid] * 100))}%")
    if data.get("thumbnail"):
        embed.set_thumbnail(url=data["thumbnail"])
    if auto:
        embed.set_footer(text="🔁 Tự động phát bài tiếp theo")
    return embed


class NowPlayingView(discord.ui.View):
    """Menu điều khiển hiện dưới tin nhắn 'Đang phát'."""

    def __init__(self, gid: int, embed: discord.Embed, data: Optional[dict] = None):
        super().__init__(timeout=None)
        self.gid = gid
        self.embed = embed
        self.base_footer = embed.footer.text

        data = data or {}
        # Nút ±10 giây cần biết độ dài bài; menu chọn đoạn dùng chương hoặc chia đều
        if not data.get("duration"):
            self.remove_item(self.back_btn)
            self.remove_item(self.fwd_btn)
        points = seek_points(data)
        if points:
            self.add_item(SeekSelect(points))

    async def interaction_check(self, inter: discord.Interaction) -> bool:
        vc = inter.guild.voice_client
        if not vc or not inter.user.voice or inter.user.voice.channel != vc.channel:
            await inter.response.send_message(
                "🎧 Hãy vào cùng kênh thoại với bot để điều khiển.", ephemeral=True
            )
            return False
        return True

    @discord.ui.button(emoji="⏮️", label="Quay lại", style=discord.ButtonStyle.secondary, row=0)
    async def prev_btn(self, inter: discord.Interaction, button: discord.ui.Button):
        await do_return(inter)

    @discord.ui.button(emoji="⏸️", label="Tạm dừng", style=discord.ButtonStyle.primary, row=0)
    async def pause_btn(self, inter: discord.Interaction, button: discord.ui.Button):
        vc = inter.guild.voice_client
        if vc.is_playing():
            vc.pause()
            button.emoji, button.label = "▶️", "Tiếp tục"
        elif vc.is_paused():
            vc.resume()
            button.emoji, button.label = "⏸️", "Tạm dừng"
        auto_paused.discard(self.gid)  # Người dùng đã tự quyết định, không tự tiếp tục nữa
        await inter.response.edit_message(view=self)

    @discord.ui.button(emoji="⏭️", label="Skip", style=discord.ButtonStyle.primary, row=0)
    async def skip_btn(self, inter: discord.Interaction, button: discord.ui.Button):
        vc = inter.guild.voice_client
        if vc.is_playing() or vc.is_paused():
            vc.stop()  # after callback sẽ gọi play_next
        await inter.response.defer()

    async def _change_volume(self, inter: discord.Interaction, delta: int):
        gid = self.gid
        percent = max(1, min(400, int(round(volumes[gid] * 100)) + delta))
        volumes[gid] = percent / 100.0

        vc = inter.guild.voice_client
        if vc and isinstance(vc.source, discord.PCMVolumeTransformer):
            vc.source.volume = volumes[gid]

        self.embed.set_field_at(2, name="🔊 Âm lượng", value=f"{percent}%")
        await inter.response.edit_message(embed=self.embed, view=self)

    @discord.ui.button(emoji="🔉", label="-10%", style=discord.ButtonStyle.secondary, row=1)
    async def vol_down_btn(self, inter: discord.Interaction, button: discord.ui.Button):
        await self._change_volume(inter, -10)

    @discord.ui.button(emoji="🔊", label="+10%", style=discord.ButtonStyle.secondary, row=1)
    async def vol_up_btn(self, inter: discord.Interaction, button: discord.ui.Button):
        await self._change_volume(inter, +10)

    @discord.ui.button(emoji="⭐", label="Yêu thích", style=discord.ButtonStyle.success, row=1)
    async def fav_btn(self, inter: discord.Interaction, button: discord.ui.Button):
        data = now_data.get(self.gid)
        if not data or not data.get("page"):
            return await inter.response.send_message(
                "❌ Không lưu được bài này vào yêu thích.", ephemeral=True
            )

        favs = favorites[inter.user.id]
        existing = next((f for f in favs if f["url"] == data["page"]), None)
        if existing:
            favs.remove(existing)
            text = f"💔 Đã bỏ **{data['title'][:100]}** khỏi yêu thích."
        else:
            favs.append({"title": data["title"], "url": data["page"]})
            text = (
                f"⭐ Đã thêm **{data['title'][:100]}** vào yêu thích. "
                "Dùng `/favorites` để xem và phát."
            )
        await inter.response.send_message(text, ephemeral=True)

    async def _seek(self, inter: discord.Interaction, seconds: float):
        data = now_data.get(self.gid)
        duration = data.get("duration") if data else None
        seconds = max(0.0, seconds)

        vc = inter.guild.voice_client
        if duration and seconds >= duration - 1:
            # Tua quá cuối bài thì chuyển sang bài tiếp
            if vc and (vc.is_playing() or vc.is_paused()):
                vc.stop()
            return await inter.response.defer()

        if not seek_to(inter.guild, seconds):
            return await inter.response.send_message(
                "❌ Không tua được lúc này.", ephemeral=True
            )

        note = f"⏩ Đã tua đến {fmt_pos(seconds)}"
        self.embed.set_footer(
            text=" • ".join(part for part in (self.base_footer, note) if part)
        )
        await inter.response.edit_message(embed=self.embed, view=self)

    async def _seek_relative(self, inter: discord.Interaction, delta: int):
        vc = inter.guild.voice_client
        position = getattr(vc.source, "position", None) if vc else None
        if position is None:
            return await inter.response.send_message(
                "❌ Chưa tua được bài này.", ephemeral=True
            )
        await self._seek(inter, position + delta)

    @discord.ui.button(emoji="⏪", label="-10s", style=discord.ButtonStyle.secondary, row=2)
    async def back_btn(self, inter: discord.Interaction, button: discord.ui.Button):
        await self._seek_relative(inter, -10)

    @discord.ui.button(emoji="⏩", label="+10s", style=discord.ButtonStyle.secondary, row=2)
    async def fwd_btn(self, inter: discord.Interaction, button: discord.ui.Button):
        await self._seek_relative(inter, +10)


class SeekSelect(discord.ui.Select):
    """Menu chọn đoạn để tua tới, không cần nhập thời gian."""

    def __init__(self, points: list):
        self.points = points
        options = [
            discord.SelectOption(label=f"{fmt_pos(t)} · {name}"[:100], value=str(i))
            for i, (name, t) in enumerate(points)
        ]
        super().__init__(placeholder="⏩ Tua đến đoạn...", options=options, row=3)

    async def callback(self, inter: discord.Interaction):
        _, seconds = self.points[int(self.values[0])]
        await self.view._seek(inter, float(seconds))


# ==================== KÊNH THOẠI TRỐNG: TẠM DỪNG / TIẾP TỤC ====================

def listener_count(channel) -> int:
    """Số người thật đang ở trong kênh thoại (không tính bot).

    Dùng voice_states thay vì channel.members, vì members chỉ có người đã nằm
    trong bộ nhớ đệm thành viên (bot này không bật members intent).
    """
    me = bot.user.id if bot.user else None
    count = 0
    for user_id in channel.voice_states:
        if user_id == me:
            continue
        member = channel.guild.get_member(user_id)
        if member is not None and member.bot:
            continue
        count += 1
    return count


async def refresh_listening(guild: discord.Guild):
    """Không còn ai trong kênh -> tạm dừng (bot vẫn ở lại). Có người quay lại -> tiếp tục."""
    gid = guild.id
    vc = guild.voice_client
    if not vc or not vc.is_connected():
        return

    if listener_count(vc.channel) == 0:
        # Chỉ tự tạm dừng khi bài đang phát; bài người dùng đã tạm dừng thì để nguyên
        if vc.is_playing():
            vc.pause()
            auto_paused.add(gid)
            print(f"[{gid}] Kênh thoại trống: tạm dừng, bot vẫn ở lại.")
        return

    if gid in auto_paused:
        auto_paused.discard(gid)
        if vc.is_paused():
            vc.resume()
            print(f"[{gid}] Có người quay lại: tiếp tục phát.")

    if gid in waiting_for_listener:
        waiting_for_listener.discard(gid)
        await play_next(guild)


# ==================== PLAY NEXT ====================

async def play_next(guild: discord.Guild, channel=None, retries: int = 0):
    """Phát bài kế tiếp: hàng đợi -> danh sách nhạc (ngẫu nhiên) -> bài gợi ý liên quan."""
    gid = guild.id
    vc = guild.voice_client
    if gid in stopping or not vc or vc.is_playing() or vc.is_paused():
        return

    if channel is None:
        channel = play_channels.get(gid)

    if channel is None:
        return

    await clear_menu(gid)

    # Kênh thoại đang trống: không phát cho không ai nghe, chờ có người vào lại
    if listener_count(vc.channel) == 0:
        waiting_for_listener.add(gid)
        return
    waiting_for_listener.discard(gid)

    q = queues[gid]
    auto_id = None  # id video nếu bài này do bot tự chọn

    if q:
        query = q.pop(0)
    elif autoplay[gid] and music_lists[gid]:
        choices = list(music_lists[gid])
        previous = current_song.get(gid)

        # Tránh phát trùng bài hiện tại nếu danh sách có nhiều lựa chọn
        if len(choices) > 1 and previous in choices:
            choices.remove(previous)

        query = random.choice(choices)
    elif autoplay[gid]:
        # Hết hàng đợi và không có danh sách nhạc: tự mở bài liên quan như YouTube
        found = await get_related_async(gid)
        if not found:
            return
        query, auto_id = found["url"], found["id"]
    else:
        return

    try:
        data = await extract_async(query)
    except Exception as exc:
        if auto_id:
            recent_ids[gid].append(auto_id)  # Không chọn lại bài lỗi này
            if retries < 2:
                return await play_next(guild, channel, retries + 1)
        print(f"Lỗi lấy bài {query[:80]!r}: {exc}")
        reason = str(exc)
        if "Sign in to confirm" in reason or "cookies" in reason.lower():
            reason = (
                "YouTube đang chặn IP của server và đòi đăng nhập. "
                "Cần thêm cookies cho bot (Secret File cookies.txt trên Render)."
            )
        try:
            await channel.send(f"❌ Không phát được `{query[:80]}`: {reason[:200]}")
        except Exception:
            pass
        if auto_id:
            return  # Bài tự chọn lỗi nhiều lần thì dừng, không thử mãi
        # Thử bài tiếp theo nếu bài này lỗi
        return await play_next(guild, channel)

    # Trong lúc tìm bài, người dùng có thể đã bắt đầu phát bài khác
    if not vc.is_connected() or vc.is_playing() or vc.is_paused():
        queues[gid].insert(0, query)
        return

    # Hoặc mọi người đã rời kênh trong lúc tìm bài: giữ bài lại, chờ có người về
    if listener_count(vc.channel) == 0:
        queues[gid].insert(0, query)
        waiting_for_listener.add(gid)
        return

    current_song[gid] = query
    now_data[gid] = data
    titles[query] = data["title"]
    history[gid].append(query)
    history[gid] = history[gid][-100:]
    if data.get("yt_id"):
        recent_ids[gid].append(data["yt_id"])
        recent_ids[gid] = recent_ids[gid][-100:]

    try:
        start_playback(guild, channel, data)
    except Exception as exc:
        print("Không thể phát:", exc)
        return await play_next(guild, channel)

    try:
        embed = build_now_playing_embed(data, gid, auto=bool(auto_id))
        now_playing_msg[gid] = await channel.send(
            embed=embed, view=NowPlayingView(gid, embed, data)
        )
    except Exception as exc:
        print("Không gửi được menu (thiếu quyền Embed Links?):", exc)


# ==================== TỰ DỌN TIN NHẮN CŨ CỦA BOT ====================

async def cleanup_channel(channel) -> int:
    """Xóa tin nhắn cũ của bot trong kênh (trừ menu đang dùng). Trả về số tin đã xóa."""
    keep = {m.id for m in now_playing_msg.values()}
    now = discord.utils.utcnow()
    deleted = 0
    async for msg in channel.history(limit=100):
        if msg.author.id != bot.user.id or msg.id in keep:
            continue
        if (now - msg.created_at).total_seconds() < CLEANUP_MIN_AGE:
            continue
        try:
            await msg.delete()
            deleted += 1
        except discord.NotFound:
            pass
    return deleted


async def cleanup_loop():
    await bot.wait_until_ready()
    while not bot.is_closed():
        await asyncio.sleep(CLEANUP_INTERVAL)
        for cid, (channel, last_used) in list(cleanup_channels.items()):
            try:
                count = await cleanup_channel(channel)
                if count:
                    print(f"Đã dọn {count} tin nhắn cũ của bot ở kênh {cid}.")
            except discord.Forbidden:
                print(f"Không đọc/xóa được tin nhắn ở kênh {cid} (thiếu quyền), bỏ qua kênh này.")
                cleanup_channels.pop(cid, None)
                continue
            except Exception as exc:
                print(f"Lỗi dọn tin nhắn ở kênh {cid}: {exc}")
            if time.time() - last_used > CLEANUP_IDLE_LIMIT:
                cleanup_channels.pop(cid, None)


# ==================== DISCORD BOT ====================

class MusicBot(discord.Client):
    def __init__(self):
        super().__init__(
            intents=discord.Intents.default(),
            chunk_guilds_at_startup=False,
        )
        self.tree = app_commands.CommandTree(self)
        self._cleanup_task = None

    async def setup_hook(self):
        # Đặt biến môi trường SYNC=0 để bỏ qua sync lệnh khi khởi động (khởi động nhanh hơn)
        if os.environ.get("SYNC", "1") != "0":
            await self.tree.sync()
        if self._cleanup_task is None:
            self._cleanup_task = asyncio.create_task(cleanup_loop())

    async def on_interaction(self, inter: discord.Interaction):
        # Ghi nhớ kênh đã dùng lệnh để tự dọn tin nhắn cũ của bot ở đó
        if inter.guild and inter.channel is not None:
            cleanup_channels[inter.channel.id] = [inter.channel, time.time()]

    async def on_voice_state_update(self, member, before, after):
        vc = member.guild.voice_client
        if not vc or not vc.is_connected():
            return
        # Chỉ quan tâm khi có người vào/ra kênh của bot, hoặc chính bot bị chuyển kênh
        if member.id != self.user.id and vc.channel not in (before.channel, after.channel):
            return
        await refresh_listening(member.guild)


bot = MusicBot()


@bot.tree.error
async def on_app_command_error(inter: discord.Interaction, error: app_commands.AppCommandError):
    """Lệnh nào bị lỗi cũng trả lời người dùng và ghi vào log."""
    print(f"Lỗi lệnh /{inter.command.name if inter.command else '?'}: {error!r}")
    msg = "❌ Có lỗi khi chạy lệnh này, hãy thử lại."
    try:
        if inter.response.is_done():
            await inter.followup.send(msg, ephemeral=True)
        else:
            await inter.response.send_message(msg, ephemeral=True)
    except Exception:
        pass


# ==================== /PLAY ====================

@bot.tree.command(name="play", description="Phát nhạc từ link hoặc tên bài hát")
@app_commands.describe(query="Link YouTube/SoundCloud/Spotify hoặc tên bài hát")
@app_commands.guild_only()
async def play(inter: discord.Interaction, query: str):
    if not inter.user.voice or not inter.user.voice.channel:
        return await inter.response.send_message(
            "🎧 Hãy vào kênh thoại trước.", ephemeral=True
        )

    await inter.response.defer()
    try:
        vc = inter.guild.voice_client
        if vc is None:
            vc = await inter.user.voice.channel.connect()
        elif not vc.is_connected():
            vc = await inter.user.voice.channel.connect()
        elif vc.channel != inter.user.voice.channel:
            await vc.move_to(inter.user.voice.channel)

        play_channels[inter.guild.id] = inter.channel

        if "open.spotify.com" in query:
            loop = asyncio.get_running_loop()
            items = await loop.run_in_executor(EXECUTOR, spotify_to_queries, query)
        else:
            items = [query]

        if not items:
            return await inter.followup.send("❌ Không tìm thấy bài hát.")

        queues[inter.guild.id].extend(items)
        await inter.followup.send(
            f"➕ Đã thêm **{len(items)} bài** vào hàng đợi."
        )

        if not vc.is_playing() and not vc.is_paused():
            await play_next(inter.guild, inter.channel)

    except Exception as exc:
        await inter.followup.send(f"❌ Có lỗi: {str(exc)[:200]}")


# ==================== /VOLUME ====================

@bot.tree.command(name="volume", description="Chỉnh âm lượng từ 1% đến 400%")
@app_commands.describe(level="Nhập âm lượng từ 1 đến 400")
@app_commands.guild_only()
async def volume(inter: discord.Interaction, level: app_commands.Range[int, 1, 400]):
    gid = inter.guild_id
    volumes[gid] = level / 100.0
    vc = inter.guild.voice_client

    if vc and vc.source and isinstance(vc.source, discord.PCMVolumeTransformer):
        vc.source.volume = volumes[gid]

    await inter.response.send_message(f"🔊 Đã chỉnh âm lượng thành **{level}%**.")


# ==================== /RETURN ====================

async def do_return(inter: discord.Interaction):
    """Quay lại bài trước (dùng cho lệnh /return và nút trên menu)."""
    gid = inter.guild_id
    vc = inter.guild.voice_client
    songs = history[gid]

    if not vc or not vc.is_connected():
        return await inter.response.send_message(
            "❌ Bot chưa ở trong kênh thoại.", ephemeral=True
        )
    if len(songs) < 2:
        return await inter.response.send_message(
            "❌ Chưa có bài trước để quay lại.", ephemeral=True
        )
    if gid in return_pending:
        return await inter.response.send_message(
            "⏳ Đang xử lý lệnh quay lại.", ephemeral=True
        )

    # Bài áp chót là bài trước bài đang phát
    previous = songs[-2]
    return_pending.add(gid)

    # Bỏ bài đang phát và bài trước khỏi lịch sử; bài trước sẽ được ghi lại khi phát,
    # nhờ vậy bấm quay lại nhiều lần vẫn lùi dần được
    songs.pop()
    songs.pop()

    # Đưa bài trước lên đầu hàng đợi, giữ nguyên những bài chờ khác
    queues[gid].insert(0, previous)
    play_channels[gid] = inter.channel

    await inter.response.send_message(
        f"⏮️ Đang quay lại bài: **{titles.get(previous, previous)[:150]}**"
    )
    try:
        if vc.is_playing() or vc.is_paused():
            vc.stop()  # after callback sẽ gọi play_next
        else:
            await play_next(inter.guild, inter.channel)
    finally:
        return_pending.discard(gid)


@bot.tree.command(name="return", description="Quay lại bài hát trước đó")
@app_commands.guild_only()
async def return_song(inter: discord.Interaction):
    await do_return(inter)


# ==================== /SKIP-TO ====================

async def resolve_titles(items: list) -> list:
    """Tên hiển thị cho các bài trong hàng đợi (link YouTube chưa biết tên thì hỏi oEmbed)."""

    async def one(session, item):
        name = titles.get(item)
        if name:
            return name
        if item.startswith("http") and ("youtube.com" in item or "youtu.be" in item):
            try:
                async with session.get(
                    OEMBED_URL, params={"url": item, "format": "json"}
                ) as resp:
                    if resp.status == 200:
                        info = await resp.json(content_type=None)
                        name = info.get("title")
                        if name:
                            titles[item] = name
                            return name
            except Exception:
                pass
        return item  # Link khác hoặc không lấy được tên: hiện nguyên văn

    global _http_session
    if _http_session is None or _http_session.closed:
        _http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=4))
    return list(await asyncio.gather(*(one(_http_session, i) for i in items)))


async def perform_skip(guild: discord.Guild, channel, index: int):
    """Chuyển thẳng đến bài ở vị trí index trong hàng đợi (0 = bài kế tiếp)."""
    gid = guild.id
    vc = guild.voice_client
    del queues[gid][:index]  # Bỏ các bài đứng trước nó
    play_channels[gid] = channel

    if vc.is_playing() or vc.is_paused():
        # after callback sẽ gọi play_next và phát bài được chọn đầu hàng đợi
        vc.stop()
    else:
        await play_next(guild, channel)


class SkipToSelect(discord.ui.Select):
    def __init__(self, items: list, names: list):
        self.items = items
        self.names = names
        options = [
            discord.SelectOption(
                label=f"{i + 1}. {name}"[:100],
                description="Bài kế tiếp" if i == 0 else f"Bỏ qua {i} bài đứng trước",
                value=str(i),
            )
            for i, name in enumerate(names)
        ]
        super().__init__(placeholder="Chọn bài muốn chuyển đến...", options=options)

    async def callback(self, inter: discord.Interaction):
        i = int(self.values[0])
        item, name = self.items[i], self.names[i]
        q = queues[inter.guild_id]
        vc = inter.guild.voice_client

        if not vc or not vc.is_connected():
            return await inter.response.edit_message(
                content="❌ Bot chưa ở trong kênh thoại.", view=None
            )
        if item not in q:
            # Hàng đợi đã đổi từ lúc mở danh sách
            return await inter.response.edit_message(
                content="❌ Bài này đã phát hoặc không còn trong hàng đợi. Hãy gõ lại `/skip-to`.",
                view=None,
            )

        await inter.response.edit_message(
            content=f"⏭️ Đang chuyển đến: **{name[:150]}**", view=None
        )
        await perform_skip(inter.guild, inter.channel, q.index(item))


class SkipToView(discord.ui.View):
    def __init__(self, items: list, names: list):
        super().__init__(timeout=120)
        self.add_item(SkipToSelect(items, names))


@bot.tree.command(
    name="skip-to",
    description="Chọn bài trong hàng đợi để chuyển thẳng đến"
)
@app_commands.describe(
    position="Số thứ tự bài (bỏ trống để hiện danh sách rồi chọn)"
)
@app_commands.guild_only()
async def skip_to(
    inter: discord.Interaction,
    position: Optional[app_commands.Range[int, 1, 100]] = None
):
    gid = inter.guild_id
    vc = inter.guild.voice_client
    q = queues[gid]

    if not vc or not vc.is_connected():
        return await inter.response.send_message(
            "❌ Bot chưa ở trong kênh thoại.", ephemeral=True
        )

    if not q:
        return await inter.response.send_message(
            "❌ Hàng đợi đang trống. Dùng `/play` để thêm bài trước.",
            ephemeral=True
        )

    if position is None:
        # Không nhập số: hiện hàng đợi kèm menu để chọn
        await inter.response.defer(ephemeral=True)
        shown = list(q[:25])
        names = await resolve_titles(shown)
        lines = "\n".join(f"{i + 1}. {name[:60]}" for i, name in enumerate(names))
        more = (
            f"\n... và {len(q) - 25} bài nữa (dùng `/skip-to position:<số>` cho các bài sau)"
            if len(q) > 25 else ""
        )
        return await inter.followup.send(
            f"📜 **Hàng đợi ({len(q)} bài)** - chọn bài muốn chuyển đến:\n{lines}{more}",
            view=SkipToView(shown, names),
            ephemeral=True,
        )

    if position > len(q):
        return await inter.response.send_message(
            f"❌ Hàng đợi chỉ có **{len(q)} bài**. Hãy chọn số từ 1 đến {len(q)}.",
            ephemeral=True
        )

    selected = q[position - 1]
    await inter.response.send_message(
        f"⏭️ Đang chuyển đến bài số **{position}** trong hàng đợi: "
        f"**{titles.get(selected, selected)[:150]}**."
    )
    await perform_skip(inter.guild, inter.channel, position - 1)


# ==================== /PAUSE ====================

@bot.tree.command(name="pause", description="Tạm dừng nhạc")
@app_commands.guild_only()
async def pause(inter: discord.Interaction):
    vc = inter.guild.voice_client
    if vc and vc.is_playing():
        vc.pause()
        auto_paused.discard(inter.guild_id)
        await inter.response.send_message("⏸️ Đã tạm dừng.")
    else:
        await inter.response.send_message(
            "❌ Không có nhạc để tạm dừng.", ephemeral=True
        )


# ==================== /RESUME ====================

@bot.tree.command(name="resume", description="Tiếp tục phát nhạc")
@app_commands.guild_only()
async def resume(inter: discord.Interaction):
    vc = inter.guild.voice_client
    if vc and vc.is_paused():
        vc.resume()
        auto_paused.discard(inter.guild_id)
        await inter.response.send_message("▶️ Tiếp tục phát nhạc.")
    else:
        await inter.response.send_message(
            "❌ Không có nhạc đang tạm dừng.", ephemeral=True
        )


# ==================== /QUEUE ====================

@bot.tree.command(name="queue", description="Xem danh sách nhạc chờ")
@app_commands.guild_only()
async def queue_cmd(inter: discord.Interaction):
    q = queues[inter.guild_id]
    if not q:
        return await inter.response.send_message(
            "📜 Hàng đợi đang trống.", ephemeral=True
        )

    text = "\n".join(f"{i + 1}. {item[:80]}" for i, item in enumerate(q[:15]))
    more = f"\n... và {len(q) - 15} bài nữa" if len(q) > 15 else ""
    await inter.response.send_message(f"📜 **Hàng đợi:**\n{text}{more}")


# ==================== /FAVORITES ====================

async def connect_voice(inter: discord.Interaction):
    """Vào (hoặc chuyển sang) kênh thoại của người dùng."""
    vc = inter.guild.voice_client
    if vc is None or not vc.is_connected():
        vc = await inter.user.voice.channel.connect()
    elif vc.channel != inter.user.voice.channel:
        await vc.move_to(inter.user.voice.channel)
    return vc


class FavoritesView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=300)

    @discord.ui.button(label="▶️ Phát tất cả", style=discord.ButtonStyle.success)
    async def play_all(self, inter: discord.Interaction, button: discord.ui.Button):
        favs = favorites[inter.user.id]
        if not favs:
            return await inter.response.send_message(
                "⭐ Danh sách yêu thích đang trống.", ephemeral=True
            )
        if not inter.user.voice or not inter.user.voice.channel:
            return await inter.response.send_message(
                "🎧 Hãy vào kênh thoại trước.", ephemeral=True
            )

        await inter.response.defer(ephemeral=True)
        try:
            gid = inter.guild_id
            vc = await connect_voice(inter)
            play_channels[gid] = inter.channel
            for f in favs:
                titles[f["url"]] = f["title"]
            queues[gid].extend(f["url"] for f in favs)
            await inter.followup.send(
                f"✅ Đã xếp **{len(favs)} bài** yêu thích vào hàng đợi.",
                ephemeral=True,
            )
            if not vc.is_playing() and not vc.is_paused():
                await play_next(inter.guild, inter.channel)
        except Exception as exc:
            await inter.followup.send(f"❌ Lỗi: {str(exc)[:200]}", ephemeral=True)

    @discord.ui.button(label="🗑️ Xóa hết", style=discord.ButtonStyle.danger)
    async def clear_all(self, inter: discord.Interaction, button: discord.ui.Button):
        favorites[inter.user.id].clear()
        await inter.response.send_message(
            "🗑️ Đã xóa toàn bộ bài yêu thích của bạn.", ephemeral=True
        )


@bot.tree.command(name="favorites", description="Xem và phát các bài yêu thích của bạn")
@app_commands.guild_only()
async def favorites_cmd(inter: discord.Interaction):
    favs = favorites[inter.user.id]
    if not favs:
        return await inter.response.send_message(
            "⭐ Bạn chưa có bài yêu thích nào. Bấm ⭐ trên menu khi bot đang phát để thêm.",
            ephemeral=True,
        )

    shown = favs[:20]
    text = "\n".join(f"{i + 1}. {f['title'][:80]}" for i, f in enumerate(shown))
    more = f"\n... và {len(favs) - 20} bài nữa" if len(favs) > 20 else ""
    await inter.response.send_message(
        f"⭐ **Bài yêu thích của bạn ({len(favs)}):**\n{text}{more}",
        view=FavoritesView(),
        ephemeral=True,
    )


# ==================== PRIVATE MUSIC LIST UI ====================

class AddSongsModal(discord.ui.Modal, title="Thêm danh sách nhạc"):
    songs = discord.ui.TextInput(
        label="Tên bài hát hoặc link (mỗi dòng một bài)",
        style=discord.TextStyle.paragraph,
        placeholder="Tên bài 1\nTên bài 2\nTên bài 3",
        max_length=4000,
        required=True,
    )

    async def on_submit(self, inter: discord.Interaction):
        gid = inter.guild_id
        if gid is None:
            return await inter.response.send_message(
                "Lệnh này chỉ dùng trong server.", ephemeral=True
            )

        added = [line.strip() for line in self.songs.value.splitlines() if line.strip()]
        music_lists[gid].extend(added)
        await inter.response.send_message(
            f"✅ Đã thêm **{len(added)} bài** vào danh sách nhạc.\n"
            f"Tổng cộng: **{len(music_lists[gid])} bài**.",
            ephemeral=True,
        )


class MusicListView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=300)

    @discord.ui.button(label="➕ Thêm bài", style=discord.ButtonStyle.primary)
    async def add_songs(self, inter: discord.Interaction, button: discord.ui.Button):
        await inter.response.send_modal(AddSongsModal())

    @discord.ui.button(label="📜 Xem danh sách", style=discord.ButtonStyle.secondary)
    async def show_songs(self, inter: discord.Interaction, button: discord.ui.Button):
        songs = music_lists[inter.guild_id]
        if not songs:
            return await inter.response.send_message(
                "Danh sách nhạc đang trống. Bấm **Thêm bài** để bắt đầu.",
                ephemeral=True,
            )

        page_size = 30
        pages = [
            songs[i:i + page_size] for i in range(0, len(songs), page_size)
        ]
        page_text = "\n".join(
            f"{i + 1}. {song[:90]}"
            for i, song in enumerate(pages[0])
        )
        await inter.response.send_message(
            f"📜 **Danh sách nhạc (1/{len(pages)} trang):**\n{page_text}",
            ephemeral=True,
        )

    @discord.ui.button(label="▶️ Xếp phát lần lượt", style=discord.ButtonStyle.success)
    async def queue_list(self, inter: discord.Interaction, button: discord.ui.Button):
        gid = inter.guild_id
        songs = music_lists[gid]
        if not songs:
            return await inter.response.send_message(
                "❌ Hãy thêm bài hát vào danh sách trước.", ephemeral=True
            )
        if not inter.user.voice or not inter.user.voice.channel:
            return await inter.response.send_message(
                "🎧 Hãy vào kênh thoại trước.", ephemeral=True
            )

        await inter.response.defer(ephemeral=True)
        try:
            vc = inter.guild.voice_client
            if vc is None:
                vc = await inter.user.voice.channel.connect()
            elif not vc.is_connected():
                vc = await inter.user.voice.channel.connect()
            elif vc.channel != inter.user.voice.channel:
                await vc.move_to(inter.user.voice.channel)

            play_channels[gid] = inter.channel
            queues[gid].extend(songs)
            await inter.followup.send(
                f"✅ Đã xếp **{len(songs)} bài** phát lần lượt.",
                ephemeral=True,
            )
            if not vc.is_playing() and not vc.is_paused():
                await play_next(inter.guild, inter.channel)
        except Exception as exc:
            await inter.followup.send(f"❌ Lỗi: {str(exc)[:200]}", ephemeral=True)

    @discord.ui.button(label="🔀 Bật/tắt tự động phát tiếp", style=discord.ButtonStyle.secondary)
    async def toggle_random(self, inter: discord.Interaction, button: discord.ui.Button):
        gid = inter.guild_id
        autoplay[gid] = not autoplay[gid]
        state = "BẬT" if autoplay[gid] else "TẮT"
        await inter.response.send_message(
            f"🔀 Tự động phát tiếp: **{state}**.\n"
            "Khi hàng đợi hết, bot sẽ chọn ngẫu nhiên từ danh sách nhạc nếu có, "
            "nếu không sẽ tự mở bài gợi ý liên quan như YouTube.",
            ephemeral=True,
        )

    @discord.ui.button(label="🗑️ Xóa danh sách", style=discord.ButtonStyle.danger)
    async def clear_list(self, inter: discord.Interaction, button: discord.ui.Button):
        music_lists[inter.guild_id].clear()
        await inter.response.send_message(
            "🗑️ Đã xóa danh sách nhạc đã lưu trong bộ nhớ.",
            ephemeral=True,
        )


@bot.tree.command(name="musiclist", description="Mở bảng quản lý danh sách nhạc riêng tư")
@app_commands.guild_only()
async def musiclist(inter: discord.Interaction):
    await inter.response.send_message(
        "🎵 **QUẢN LÝ DANH SÁCH NHẠC**\n"
        "Thêm nhiều bài, xem danh sách, xếp phát lần lượt hoặc bật/tắt tự động phát tiếp.\n"
        "Bảng này chỉ mình bạn nhìn thấy.",
        view=MusicListView(),
        ephemeral=True,
    )


# ==================== /STOP ====================

@bot.tree.command(name="stop", description="Dừng nhạc, xóa hàng đợi và thoát kênh")
@app_commands.guild_only()
async def stop(inter: discord.Interaction):
    gid = inter.guild_id
    queues[gid].clear()
    # Trả lời ngay: ngắt kênh thoại có thể lâu hơn 3 giây, Discord sẽ báo "không phản hồi"
    await inter.response.send_message("⏹️ Đang dừng nhạc và thoát kênh thoại...")
    stopping.add(gid)  # Chặn play_next tự phát bài mới trong lúc đang dừng
    vc = inter.guild.voice_client

    try:
        if vc:
            if vc.is_playing() or vc.is_paused():
                vc.stop()
            await vc.disconnect()
    finally:
        stopping.discard(gid)

    current_song.pop(gid, None)
    now_data.pop(gid, None)
    auto_paused.discard(gid)
    waiting_for_listener.discard(gid)
    await clear_menu(gid)

# ==================== /SEARCH ====================

def search_youtube(query: str, limit: int = 10) -> list[dict]:
    """Tìm trên YouTube, chỉ lấy thông tin cơ bản (nhanh, không tải gì)."""
    opts = {"quiet": True, "no_warnings": True, "extract_flat": True, **COOKIE_OPTS}
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(f"ytsearch{limit}:{query}", download=False)

    results = []
    for e in (info or {}).get("entries") or []:
        if not e or not e.get("id"):
            continue
        results.append({
            "title": e.get("title") or "Không rõ tên",
            "url": f"https://www.youtube.com/watch?v={e['id']}",
            "channel": e.get("channel") or e.get("uploader") or "?",
            "duration": e.get("duration"),
        })
    return results


def fmt_duration(sec) -> str:
    if not sec:
        return "--:--"
    sec = int(sec)
    h, rest = divmod(sec, 3600)
    m, s = divmod(rest, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


class SearchSelect(discord.ui.Select):
    def __init__(self, results: list[dict]):
        self.results = results
        options = [
            discord.SelectOption(
                label=f"{i + 1}. {r['title']}"[:100],
                description=f"{r['channel']} • {fmt_duration(r['duration'])}"[:100],
                value=str(i),
            )
            for i, r in enumerate(results)
        ]
        super().__init__(placeholder="Chọn bài để phát...", options=options)

    async def callback(self, inter: discord.Interaction):
        gid = inter.guild_id
        chosen = self.results[int(self.values[0])]

        if not inter.user.voice or not inter.user.voice.channel:
            return await inter.response.send_message(
                "🎧 Hãy vào kênh thoại trước.", ephemeral=True
            )

        await inter.response.defer()
        try:
            vc = inter.guild.voice_client
            if vc is None or not vc.is_connected():
                vc = await inter.user.voice.channel.connect()
            elif vc.channel != inter.user.voice.channel:
                await vc.move_to(inter.user.voice.channel)

            play_channels[gid] = inter.channel
            queues[gid].append(chosen["url"])
            titles[chosen["url"]] = chosen["title"]
            self.view.stop()
            await inter.edit_original_response(
                content=f"✅ Đã chọn: **{chosen['title']}**", view=None
            )
            if not vc.is_playing() and not vc.is_paused():
                await play_next(inter.guild, inter.channel)
        except Exception as exc:
            await inter.followup.send(f"❌ Lỗi: {str(exc)[:200]}", ephemeral=True)


class SearchView(discord.ui.View):
    def __init__(self, results: list[dict], author_id: int):
        super().__init__(timeout=60)
        self.author_id = author_id
        self.message = None
        self.add_item(SearchSelect(results))

    async def interaction_check(self, inter: discord.Interaction) -> bool:
        if inter.user.id != self.author_id:
            await inter.response.send_message(
                "Chỉ người gọi lệnh mới chọn được.", ephemeral=True
            )
            return False
        return True

    async def on_timeout(self):
        if self.message:
            try:
                await self.message.delete()  # Hết hạn, không còn dùng được nữa
            except Exception:
                pass


@bot.tree.command(name="search", description="Tìm bài trên YouTube rồi chọn bài để phát")
@app_commands.describe(query="Tên bài hát hoặc từ khóa tìm kiếm")
@app_commands.guild_only()
async def search(inter: discord.Interaction, query: str):
    if not inter.user.voice or not inter.user.voice.channel:
        return await inter.response.send_message(
            "🎧 Hãy vào kênh thoại trước.", ephemeral=True
        )

    await inter.response.defer()
    loop = asyncio.get_running_loop()
    try:
        results = await loop.run_in_executor(None, search_youtube, query)
    except Exception as exc:
        return await inter.followup.send(f"❌ Không tìm được: {str(exc)[:200]}")

    if not results:
        return await inter.followup.send("❌ Không có kết quả nào.")

    lines = "\n".join(
        f"**{i + 1}.** {r['title'][:80]} — {r['channel'][:30]} `{fmt_duration(r['duration'])}`"
        for i, r in enumerate(results)
    )
    view = SearchView(results, inter.user.id)
    view.message = await inter.followup.send(
        f"🔎 Kết quả cho **{query[:80]}**:\n{lines}", view=view, wait=True
    )

# ==================== RUN BOT (RENDER) ====================

async def start_web_server():
    """Web server nhỏ để Render nhận diện service và để UptimeRobot ping."""
    async def health(request):
        status = "online" if bot.is_ready() else "connecting"
        return web.Response(text=f"Music bot is running (discord: {status})")

    app = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 10000))
    await web.TCPSite(runner, "0.0.0.0", port).start()
    print(f"Web server đang chạy ở cổng {port}")


async def main():
    if not TOKEN:
        raise ValueError("Không tìm thấy biến môi trường DISCORD_TOKEN trên Render.")

    await start_web_server()

    delay = 60
    while True:
        try:
            await bot.login(TOKEN)
            break
        except discord.LoginFailure:
            raise  # Token sai thì thử lại cũng vô ích
        except discord.HTTPException as exc:
            if exc.status != 429:
                raise
            print(f"Discord đang chặn IP của server (429). Thử lại sau {delay} giây...")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 900)

    try:
        await bot.connect(reconnect=True)
    finally:
        await bot.close()


if __name__ == "__main__":
    asyncio.run(main())
