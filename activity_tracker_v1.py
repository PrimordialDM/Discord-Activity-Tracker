# HOW TO USE
# 1) Install: pip install -U discord.py aiosqlite python-dotenv
# 2) Create a bot in the Discord Developer Portal. Invite with scopes: bot, applications.commands.
# 3) In Bot settings, enable Intents: Message Content, Server Members, Guilds.
# 4) Create .env with:
#    DISCORD_TOKEN=your_bot_token
#    GUILD_ID=optional_single_guild_id_for_fast_sync
#    REPORTER_ROLE_ID=optional_bootstrap_reporter_role_id
#    REPORTER_ROLE_NAME=optional_bootstrap_reporter_role_name
#    ALLOWED_ROLE_IDS=optional_comma_separated_role_ids
#    ALLOWED_ROLE_NAMES=optional_comma_separated_role_names
# 5) Pray to the machine spirit that the bot works.
# 6) Configure per server:
#    /activity setchannel #channel
#    /activity set_reporter_role @Reporters
#    /activity allow_role_add @Mods
# 7) Reporting (only reporter role):
#    /activity report user:@User operation:"Operation Name" active_participant:true|false
#    /activity report_bulk lines:"<id-or-mention> - Operation - Yes/No\n<id-or-mention> - Operation - Yes/No"
#    Or paste lines in the configured channel
# 8) Query and admin:
#    /activity leaderboard period:all|30d|7d|1d limit:N
#    /activity inactive days:N
#    /activity user user:@User
#    /activity export period:all|30d|7d|1d
#    /activity delete_entry message_id line_no
#    /activity delete_entries message_id start_line end_line
#    /activity delete_user user:@User
#    /activity purge_all confirm:true
#    /activity reindex

#   POST TESTING NOTE 1: **DO NOT** SET A CHANNEL TO PRIVATE/READ-ONLY FOR THE BOT.
#   POST TESTING NOTE 2: IF YOU SET A CHANNEL, IT'LL NOT RESPOND TO COMMANDS OUTSIDE IT. NOT CHANGEABLE LATER.

#   Prim's Personal Note: It works. Yes. I am surprised too. Please don't break it. :D

from __future__ import annotations
import asyncio
import csv
import io
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple, Iterable

import aiosqlite
import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
DB_PATH = os.getenv("DB_PATH", "activity.sqlite3")
REPORTER_ROLE_NAME = os.getenv("REPORTER_ROLE_NAME", "").strip()
REPORTER_ROLE_ID = int(os.getenv("REPORTER_ROLE_ID", "0") or 0)
ALLOWED_ROLE_IDS = {int(x) for x in os.getenv("ALLOWED_ROLE_IDS", "").split(",") if x.strip().isdigit()}
ALLOWED_ROLE_NAMES = {x.strip() for x in os.getenv("ALLOWED_ROLE_NAMES", "").split(",") if x.strip()}
GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)

INTENTS = discord.Intents.default()
INTENTS.message_content = True
INTENTS.members = True
INTENTS.guilds = True

USER_ID_RE = re.compile(r"^\s*(?:<@!?(?P<mention>\d{17,20})>|(?P<raw>\d{17,20}))\s*-\s*(?P<op>.+?)\s*-\s*(?P<flag>yes|no|y|n)\s*$", re.IGNORECASE)

@dataclass
class ParsedEntry:
    user_id: int
    op_name: str
    active_participant: bool
    line_no: int

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def to_unix(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())

def parse_iso_to_unix(s: str) -> Optional[int]:
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        try:
            return int(s)
        except Exception:
            return None

class ActivityStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._db: Optional[aiosqlite.Connection] = None

    async def init(self) -> None:
        self._db = await aiosqlite.connect(self.db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.executescript(
            """
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS activities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                line_no INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                reporter_id INTEGER NOT NULL,
                op_name TEXT NOT NULL,
                leading_ship INTEGER NOT NULL,
                ts_unix INTEGER NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_msg_line ON activities(message_id, line_no);
            CREATE INDEX IF NOT EXISTS idx_activities_user_ts ON activities(user_id, ts_unix);
            CREATE INDEX IF NOT EXISTS idx_activities_guild_ts ON activities(guild_id, ts_unix);
            CREATE INDEX IF NOT EXISTS idx_activities_guild_user ON activities(guild_id, user_id);
            CREATE INDEX IF NOT EXISTS idx_activities_guild_msg_line ON activities(guild_id, message_id, line_no);
            CREATE TABLE IF NOT EXISTS user_last (
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                last_ts_unix INTEGER NOT NULL,
                last_op_name TEXT,
                PRIMARY KEY(guild_id, user_id)
            );
            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id INTEGER PRIMARY KEY,
                activity_channel_id INTEGER,
                reporter_role_id INTEGER,
                allowed_role_ids TEXT
            );
            """
        )
        await self._db.commit()
        await self.migrate_old_schema()

    async def migrate_old_schema(self) -> None:
        assert self._db is not None
        try:
            async with self._db.execute("PRAGMA table_info(activities)") as cur:
                cols = [row[1] for row in await cur.fetchall()]
            if "ts_unix" not in cols:
                await self._db.execute("ALTER TABLE activities ADD COLUMN ts_unix INTEGER")
            if "ts_utc" in cols:
                async with self._db.execute("SELECT id, ts_utc FROM activities") as cur:
                    rows = await cur.fetchall()
                for row in rows:
                    unix = parse_iso_to_unix(row[1])
                    if unix is not None:
                        await self._db.execute("UPDATE activities SET ts_unix=? WHERE id=?", (unix, row[0]))
            await self._db.executescript(
                """
                CREATE INDEX IF NOT EXISTS idx_activities_user_ts ON activities(user_id, ts_unix);
                CREATE INDEX IF NOT EXISTS idx_activities_guild_ts ON activities(guild_id, ts_unix);
                CREATE INDEX IF NOT EXISTS idx_activities_guild_user ON activities(guild_id, user_id);
                CREATE INDEX IF NOT EXISTS idx_activities_guild_msg_line ON activities(guild_id, message_id, line_no);
                """
            )
            async with self._db.execute("PRAGMA table_info(user_last)") as cur:
                ucols = [row[1] for row in await cur.fetchall()]
            if set(ucols) != {"guild_id", "user_id", "last_ts_unix", "last_op_name"}:
                await self._db.execute("DROP TABLE IF EXISTS user_last")
                await self._db.execute(
                    """
                    CREATE TABLE user_last (
                        guild_id INTEGER NOT NULL,
                        user_id INTEGER NOT NULL,
                        last_ts_unix INTEGER NOT NULL,
                        last_op_name TEXT,
                        PRIMARY KEY(guild_id, user_id)
                    );
                    """
                )
                await self._db.execute("DELETE FROM user_last")
                await self._db.execute(
                    """
                    INSERT INTO user_last(guild_id, user_id, last_ts_unix, last_op_name)
                    SELECT a.guild_id, a.user_id, MAX(a.ts_unix),
                           (SELECT a2.op_name FROM activities a2 WHERE a2.guild_id=a.guild_id AND a2.user_id=a.user_id ORDER BY a2.ts_unix DESC LIMIT 1)
                    FROM activities a
                    GROUP BY a.guild_id, a.user_id
                    """
                )
            await self._db.commit()
        except Exception:
            pass

    async def close(self) -> None:
        if self._db:
            await self._db.close()

    async def get_settings(self, guild_id: int) -> dict:
        assert self._db is not None
        async with self._db.execute(
            "SELECT activity_channel_id, reporter_role_id, allowed_role_ids FROM guild_settings WHERE guild_id=?",
            (guild_id,),
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return {"activity_channel_id": None, "reporter_role_id": None, "allowed_role_ids": []}
        allowed = [int(x) for x in (row["allowed_role_ids"] or "").split(",") if x.strip().isdigit()]
        return {
            "activity_channel_id": row["activity_channel_id"],
            "reporter_role_id": row["reporter_role_id"],
            "allowed_role_ids": allowed,
        }

    async def set_activity_channel(self, guild_id: int, channel_id: int) -> None:
        assert self._db is not None
        await self._db.execute(
            """
            INSERT INTO guild_settings(guild_id, activity_channel_id)
            VALUES(?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET activity_channel_id=excluded.activity_channel_id
            """,
            (guild_id, channel_id),
        )
        await self._db.commit()

    async def set_reporter_role(self, guild_id: int, role_id: int) -> None:
        assert self._db is not None
        await self._db.execute(
            """
            INSERT INTO guild_settings(guild_id, reporter_role_id)
            VALUES(?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET reporter_role_id=excluded.reporter_role_id
            """,
            (guild_id, role_id),
        )
        await self._db.commit()

    async def add_allowed_role(self, guild_id: int, role_id: int) -> None:
        st = await self.get_settings(guild_id)
        ids = set(st["allowed_role_ids"]) | {role_id}
        csv_ids = ",".join(str(x) for x in sorted(ids))
        assert self._db is not None
        await self._db.execute(
            """
            INSERT INTO guild_settings(guild_id, allowed_role_ids)
            VALUES(?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET allowed_role_ids=excluded.allowed_role_ids
            """,
            (guild_id, csv_ids),
        )
        await self._db.commit()

    async def remove_allowed_role(self, guild_id: int, role_id: int) -> None:
        st = await self.get_settings(guild_id)
        ids = [x for x in st["allowed_role_ids"] if x != role_id]
        csv_ids = ",".join(str(x) for x in ids)
        assert self._db is not None
        await self._db.execute(
            """
            INSERT INTO guild_settings(guild_id, allowed_role_ids)
            VALUES(?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET allowed_role_ids=excluded.allowed_role_ids
            """,
            (guild_id, csv_ids),
        )
        await self._db.commit()

    async def clear_allowed_roles(self, guild_id: int) -> None:
        assert self._db is not None
        await self._db.execute(
            """
            INSERT INTO guild_settings(guild_id, allowed_role_ids)
            VALUES(?, '')
            ON CONFLICT(guild_id) DO UPDATE SET allowed_role_ids=''
            """,
            (guild_id,),
        )
        await self._db.commit()

    async def upsert_entries(
        self,
        guild_id: int,
        channel_id: int,
        message_id: int,
        reporter_id: int,
        entries: List[ParsedEntry],
        ts: datetime,
    ) -> Tuple[int, int]:
        assert self._db is not None
        inserted = 0
        updated = 0
        unix = to_unix(ts)
        for e in entries:
            try:
                await self._db.execute(
                    """
                    INSERT INTO activities
                    (guild_id, channel_id, message_id, line_no, user_id, reporter_id, op_name, leading_ship, ts_unix)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        guild_id,
                        channel_id,
                        message_id,
                        e.line_no,
                        e.user_id,
                        reporter_id,
                        e.op_name.strip(),
                        1 if e.active_participant else 0,
                        unix,
                    ),
                )
                inserted += 1
            except aiosqlite.IntegrityError:
                await self._db.execute(
                    """
                    UPDATE activities
                    SET user_id=?, reporter_id=?, op_name=?, leading_ship=?, ts_unix=?
                    WHERE guild_id=? AND message_id=? AND line_no=?
                    """,
                    (
                        e.user_id,
                        reporter_id,
                        e.op_name.strip(),
                        1 if e.active_participant else 0,
                        unix,
                        guild_id,
                        message_id,
                        e.line_no,
                    ),
                )
                updated += 1
            await self._db.execute(
                """
                INSERT INTO user_last(guild_id, user_id, last_ts_unix, last_op_name)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(guild_id, user_id) DO UPDATE SET
                    last_ts_unix=excluded.last_ts_unix,
                    last_op_name=excluded.last_op_name
                WHERE excluded.last_ts_unix > user_last.last_ts_unix
                """,
                (guild_id, e.user_id, unix, e.op_name.strip()),
            )
        await self._db.commit()
        return inserted, updated

    async def leaderboard(self, guild_id: int, since_utc: Optional[datetime], limit: int) -> List[aiosqlite.Row]:
        assert self._db is not None
        if since_utc is None:
            q = "SELECT user_id, COUNT(*) AS cnt FROM activities WHERE guild_id=? GROUP BY user_id ORDER BY cnt DESC, user_id ASC LIMIT ?"
            args = (guild_id, limit)
        else:
            cutoff = int(since_utc.timestamp())
            q = "SELECT user_id, COUNT(*) AS cnt FROM activities WHERE guild_id=? AND ts_unix>=? GROUP BY user_id ORDER BY cnt DESC, user_id ASC LIMIT ?"
            args = (guild_id, cutoff, limit)
        async with self._db.execute(q, args) as cur:
            return await cur.fetchall()

    async def user_activity(self, guild_id: int, user_id: int, since_utc: Optional[datetime], limit: int = 20) -> List[aiosqlite.Row]:
        assert self._db is not None
        if since_utc is None:
            q = "SELECT op_name, leading_ship, ts_unix, reporter_id FROM activities WHERE guild_id=? AND user_id=? ORDER BY ts_unix DESC LIMIT ?"
            args = (guild_id, user_id, limit)
        else:
            cutoff = int(since_utc.timestamp())
            q = "SELECT op_name, leading_ship, ts_unix, reporter_id FROM activities WHERE guild_id=? AND user_id=? AND ts_unix>=? ORDER BY ts_unix DESC LIMIT ?"
            args = (guild_id, user_id, cutoff, limit)
        async with self._db.execute(q, args) as cur:
            return await cur.fetchall()

    async def inactive_since(self, guild_id: int, cutoff_utc: datetime) -> List[aiosqlite.Row]:
        assert self._db is not None
        cutoff = int(cutoff_utc.timestamp())
        q = "SELECT user_id, last_ts_unix, last_op_name FROM user_last WHERE guild_id=? AND last_ts_unix < ? ORDER BY last_ts_unix ASC"
        async with self._db.execute(q, (guild_id, cutoff)) as cur:
            return await cur.fetchall()

    async def export_rows(self, guild_id: int, since_utc: Optional[datetime]) -> Iterable[Tuple]:
        assert self._db is not None
        if since_utc is None:
            q = "SELECT guild_id, channel_id, message_id, line_no, user_id, reporter_id, op_name, leading_ship, ts_unix FROM activities WHERE guild_id=? ORDER BY ts_unix ASC"
            args = (guild_id,)
        else:
            cutoff = int(since_utc.timestamp())
            q = "SELECT guild_id, channel_id, message_id, line_no, user_id, reporter_id, op_name, leading_ship, ts_unix FROM activities WHERE guild_id=? AND ts_unix>=? ORDER BY ts_unix ASC"
            args = (guild_id, cutoff)
        async with self._db.execute(q, args) as cur:
            async for row in cur:
                yield (row[0], row[1], row[2], row[3], row[4], row[5], row[6], bool(row[7]), row[8])

    async def delete_by_msg_line(self, guild_id: int, message_id: int, line_no: int) -> int:
        assert self._db is not None
        users = []
        async with self._db.execute(
            "SELECT user_id FROM activities WHERE guild_id=? AND message_id=? AND line_no=?",
            (guild_id, message_id, line_no),
        ) as cur:
            users = [r[0] for r in await cur.fetchall()]
        await self._db.execute("DELETE FROM activities WHERE guild_id=? AND message_id=? AND line_no=?", (guild_id, message_id, line_no))
        await self._db.commit()
        for uid in set(users):
            await self.rebuild_user_last_for_user(guild_id, uid)
        return len(users)

    async def delete_range_by_msg(self, guild_id: int, message_id: int, start_line: int, end_line: int) -> int:
        assert self._db is not None
        users = []
        async with self._db.execute(
            "SELECT DISTINCT user_id FROM activities WHERE guild_id=? AND message_id=? AND line_no BETWEEN ? AND ?",
            (guild_id, message_id, start_line, end_line),
        ) as cur:
            users = [r[0] for r in await cur.fetchall()]
        await self._db.execute(
            "DELETE FROM activities WHERE guild_id=? AND message_id=? AND line_no BETWEEN ? AND ?",
            (guild_id, message_id, start_line, end_line),
        )
        await self._db.commit()
        for uid in set(users):
            await self.rebuild_user_last_for_user(guild_id, uid)
        return len(users)

    async def delete_user_in_guild(self, guild_id: int, user_id: int) -> int:
        assert self._db is not None
        await self._db.execute("DELETE FROM activities WHERE guild_id=? AND user_id=?", (guild_id, user_id))
        await self._db.commit()
        await self.rebuild_user_last_for_user(guild_id, user_id)
        return 1

    async def purge_guild(self, guild_id: int) -> None:
        assert self._db is not None
        await self._db.execute("DELETE FROM activities WHERE guild_id=?", (guild_id,))
        await self._db.commit()
        await self.rebuild_user_last_all_for_guild(guild_id)

    async def rebuild_user_last_for_user(self, guild_id: int, user_id: int) -> None:
        assert self._db is not None
        async with self._db.execute(
            "SELECT ts_unix, op_name FROM activities WHERE guild_id=? AND user_id=? ORDER BY ts_unix DESC LIMIT 1",
            (guild_id, user_id),
        ) as cur:
            row = await cur.fetchone()
        if row:
            await self._db.execute(
                "INSERT INTO user_last(guild_id, user_id, last_ts_unix, last_op_name) VALUES(?, ?, ?, ?) "
                "ON CONFLICT(guild_id, user_id) DO UPDATE SET last_ts_unix=excluded.last_ts_unix, last_op_name=excluded.last_op_name",
                (guild_id, user_id, row[0], row[1]),
            )
        else:
            await self._db.execute("DELETE FROM user_last WHERE guild_id=? AND user_id=?", (guild_id, user_id))
        await self._db.commit()

    async def rebuild_user_last_all_for_guild(self, guild_id: int) -> None:
        assert self._db is not None
        await self._db.execute("DELETE FROM user_last WHERE guild_id=?", (guild_id,))
        await self._db.execute(
            """
            INSERT INTO user_last(guild_id, user_id, last_ts_unix, last_op_name)
            SELECT a.guild_id, a.user_id, MAX(a.ts_unix),
                   (SELECT a2.op_name FROM activities a2 WHERE a2.guild_id=a.guild_id AND a2.user_id=a.user_id ORDER BY a2.ts_unix DESC LIMIT 1)
            FROM activities a
            WHERE a.guild_id=?
            GROUP BY a.guild_id, a.user_id
            """,
            (guild_id,),
        )
        await self._db.commit()

async def fetch_settings_for(interaction: discord.Interaction) -> dict:
    return await interaction.client.store.get_settings(interaction.guild.id)

def member_has_allowed_role(member: discord.Member, st: dict | None) -> bool:
    allow_ids = set((st or {}).get("allowed_role_ids", [])) or ALLOWED_ROLE_IDS
    allow_names = ALLOWED_ROLE_NAMES
    if not allow_ids and not allow_names:
        return True
    return any((r.id in allow_ids) or (r.name in allow_names) for r in member.roles)

def member_has_reporter_role(member: discord.Member, st: dict | None) -> bool:
    reporter_role_id = (st or {}).get("reporter_role_id")
    if reporter_role_id:
        return any(r.id == reporter_role_id for r in member.roles)
    if REPORTER_ROLE_ID:
        return any(r.id == REPORTER_ROLE_ID for r in member.roles)
    if REPORTER_ROLE_NAME:
        return any(r.name == REPORTER_ROLE_NAME for r in member.roles)
    return False

def ensure_access():
    async def predicate(interaction: discord.Interaction) -> bool:
        if not interaction.guild:
            raise app_commands.CheckFailure("Use this in a server.")
        member = interaction.user if isinstance(interaction.user, discord.Member) else await interaction.guild.fetch_member(interaction.user.id)
        st = await fetch_settings_for(interaction)
        if not member_has_allowed_role(member, st):
            raise app_commands.CheckFailure("You do not have permission to use this command.")
        return True
    return app_commands.check(predicate)

def ensure_reporter():
    async def predicate(interaction: discord.Interaction) -> bool:
        if not interaction.guild:
            raise app_commands.CheckFailure("Use this in a server.")
        member = interaction.user if isinstance(interaction.user, discord.Member) else await interaction.guild.fetch_member(interaction.user.id)
        st = await fetch_settings_for(interaction)
        if not member_has_reporter_role(member, st):
            raise app_commands.CheckFailure("You must have the reporter role to log reports.")
        return True
    return app_commands.check(predicate)

class ActivityCog(commands.Cog):
    def __init__(self, bot: commands.Bot, store: ActivityStore):
        self.bot = bot
        self.store = store

    async def cog_app_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.CheckFailure):
            try:
                msg = str(error) or "You do not have permission to use this command."
                if interaction.response.is_done():
                    await interaction.followup.send(msg, ephemeral=True)
                else:
                    await interaction.response.send_message(msg, ephemeral=True)
            except Exception:
                pass
            return
        raise error

    @staticmethod
    def parse_message(content: str) -> List[ParsedEntry]:
        entries: List[ParsedEntry] = []
        for idx, raw_line in enumerate(content.splitlines(), start=1):
            line = raw_line.strip()
            if not line:
                continue
            m = USER_ID_RE.match(line)
            if not m:
                continue
            user_id = int(m.group("mention") or m.group("raw"))
            op_name = m.group("op").strip()
            flag_raw = m.group("flag").lower()
            active = flag_raw in {"yes", "y"}
            entries.append(ParsedEntry(user_id=user_id, op_name=op_name, active_participant=active, line_no=idx))
        return entries

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or not message.guild:
            return
        st = await self.store.get_settings(message.guild.id)
        activity_channel_id = st["activity_channel_id"]
        if activity_channel_id and message.channel.id != activity_channel_id:
            return
        if not isinstance(message.author, discord.Member):
            return
        if not member_has_reporter_role(message.author, st):
            return
        entries = self.parse_message(message.content)
        if not entries:
            return
        ts = message.created_at if message.created_at.tzinfo else message.created_at.replace(tzinfo=timezone.utc)
        await self.store.upsert_entries(
            guild_id=message.guild.id,
            channel_id=message.channel.id,
            message_id=message.id,
            reporter_id=message.author.id,
            entries=entries,
            ts=ts,
        )
        try:
            await message.add_reaction("✅")
        except discord.HTTPException:
            pass

    @commands.Cog.listener())
    async def on_message_edit(self, before: discord.Message, after: discord.Message):
        if after.author.bot or not after.guild:
            return
        st = await self.store.get_settings(after.guild.id)
        activity_channel_id = st["activity_channel_id"]
        if activity_channel_id and after.channel.id != activity_channel_id:
            return
        if not isinstance(after.author, discord.Member):
            return
        if not member_has_reporter_role(after.author, st):
            return
        entries = self.parse_message(after.content)
        if not entries:
            return
        ts = after.edited_at or utcnow()
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        await self.store.upsert_entries(
            guild_id=after.guild.id,
            channel_id=after.channel.id,
            message_id=after.id,
            reporter_id=after.author.id,
            entries=entries,
            ts=ts,
        )
        try:
            await after.add_reaction("✏️")
        except discord.HTTPException:
            pass

    activity = app_commands.Group(name="activity", description="Activity tracking commands")

    @app_commands.default_permissions(manage_guild=True)
    @activity.command(name="setchannel", description="Set the channel where activity reports are tracked (persistent)")
    async def setchannel(self, interaction: discord.Interaction, channel: discord.TextChannel):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("Manage Server permission required.", ephemeral=True)
            return
        await self.store.set_activity_channel(interaction.guild.id, channel.id)
        await interaction.response.send_message(f"Activity tracking channel set to {channel.mention}", ephemeral=True)

    @app_commands.default_permissions(manage_guild=True, manage_roles=True)
    @activity.command(name="set_reporter_role", description="Set which role may submit reports (persistent)")
    async def set_reporter_role(self, interaction: discord.Interaction, role: discord.Role):
        if not interaction.user.guild_permissions.manage_guild and not interaction.user.guild_permissions.manage_roles:
            await interaction.response.send_message("Manage Server or Manage Roles required.", ephemeral=True)
            return
        await self.store.set_reporter_role(interaction.guild.id, role.id)
        await interaction.response.send_message(f"Reporter role set to {role.mention}", ephemeral=True)

    @app_commands.default_permissions(manage_guild=True)
    @activity.command(name="allow_role_add", description="Allow a role to use bot commands (persistent)")
    async def allow_role_add(self, interaction: discord.Interaction, role: discord.Role):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("Manage Server permission required.", ephemeral=True)
            return
        await self.store.add_allowed_role(interaction.guild.id, role.id)
        await interaction.response.send_message(f"Added {role.mention} to allowed roles.", ephemeral=True)

    @app_commands.default_permissions(manage_guild=True)
    @activity.command(name="allow_role_remove", description="Remove a role from bot command access (persistent)")
    async def allow_role_remove(self, interaction: discord.Interaction, role: discord.Role):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("Manage Server permission required.", ephemeral=True)
            return
        await self.store.remove_allowed_role(interaction.guild.id, role.id)
        await interaction.response.send_message(f"Removed {role.mention} from allowed roles.", ephemeral=True)

    @app_commands.default_permissions(manage_guild=True)
    @activity.command(name="allow_role_clear", description="Clear all allowed roles (reverts to allow-all unless env set)")
    async def allow_role_clear(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("Manage Server permission required.", ephemeral=True)
            return
        await self.store.clear_allowed_roles(interaction.guild.id)
        await interaction.response.send_message("Cleared allowed roles.", ephemeral=True)

    @app_commands.default_permissions(manage_guild=True)
    @activity.command(name="allow_role_list", description="Show currently allowed roles")
    async def allow_role_list(self, interaction: discord.Interaction):
        st = await self.store.get_settings(interaction.guild.id)
        roles = [interaction.guild.get_role(rid) for rid in st["allowed_role_ids"]]
        role_list = ", ".join(r.mention for r in roles if r) or "<none>"
        await interaction.response.send_message(f"Allowed roles: {role_list}", ephemeral=True)

    @app_commands.default_permissions(manage_guild=True)
    @activity.command(name="settings", description="Show current guild settings")
    async def settings(self, interaction: discord.Interaction):
        st = await self.store.get_settings(interaction.guild.id)
        chan = interaction.guild.get_channel(st["activity_channel_id"]) if st["activity_channel_id"] else None
        rep = interaction.guild.get_role(st["reporter_role_id"]) if st["reporter_role_id"] else None
        roles = [interaction.guild.get_role(rid) for rid in st["allowed_role_ids"]]
        role_list = ", ".join(r.mention for r in roles if r) or "<none>"
        await interaction.response.send_message(
            f"Channel: {chan.mention if chan else '<none>'}\nReporter role: {rep.mention if rep else '<none>'}\nAllowed roles: {role_list}",
            ephemeral=True,
        )

    @ensure_access()
    @ensure_reporter()
    @activity.command(name="report", description="Log an activity entry for a user")
    @app_commands.describe(user="The user being credited", operation="Operation name", active_participant="Were they an active participant?")
    async def report(self, interaction: discord.Interaction, user: discord.User, operation: str, active_participant: bool):
        st = await self.store.get_settings(interaction.guild.id)
        chan_id = st["activity_channel_id"]
        if chan_id and interaction.channel.id != chan_id:
            chan = interaction.guild.get_channel(chan_id)
            await interaction.response.send_message(f"Please use this in {chan.mention if chan else f'<#{chan_id}>'}.", ephemeral=True)
            return
        ts = discord.utils.snowflake_time(interaction.id)
        entry = ParsedEntry(user_id=user.id, op_name=operation, active_participant=active_participant, line_no=1)
        await self.store.upsert_entries(
            guild_id=interaction.guild.id,
            channel_id=interaction.channel.id,
            message_id=interaction.id,
            reporter_id=interaction.user.id,
            entries=[entry],
            ts=ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc),
        )
        unix = to_unix(ts)
        await interaction.response.send_message(
            f"Logged for {user.mention}: {operation} — Active Participant: {'Yes' if active_participant else 'No'} — <t:{unix}:F>",
            ephemeral=True,
        )

    @ensure_access()
    @ensure_reporter()
    @activity.command(name="report_bulk", description="Log multiple entries from pasted lines")
    @app_commands.describe(lines="One per line: <user-id or mention> - Operation Name - Yes/No (Active Participant)")
    async def report_bulk(self, interaction: discord.Interaction, lines: str):
        st = await self.store.get_settings(interaction.guild.id)
        chan_id = st["activity_channel_id"]
        if chan_id and interaction.channel.id != chan_id:
            chan = interaction.guild.get_channel(chan_id)
            await interaction.response.send_message(f"Please use this in {chan.mention if chan else f'<#{chan_id}>'}.", ephemeral=True)
            return
        entries = self.parse_message(lines)
        if not entries:
            await interaction.response.send_message("No valid lines found. Format: `<user-id or mention> - Operation Name - Yes/No` (Active Participant)", ephemeral=True)
            return
        ts = discord.utils.snowflake_time(interaction.id)
        inserted, updated = await self.store.upsert_entries(
            guild_id=interaction.guild.id,
            channel_id=interaction.channel.id,
            message_id=interaction.id,
            reporter_id=interaction.user.id,
            entries=entries,
            ts=ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc),
        )
        unix = to_unix(ts)
        await interaction.response.send_message(f"Processed {len(entries)} line(s). Inserted {inserted}, updated {updated}. — <t:{unix}:F>", ephemeral=True)

    @ensure_access()
    @activity.command(name="leaderboard", description="Show top users by report count")
    @app_commands.describe(period="Time window", limit="Number of users to display")
    @app_commands.choices(period=[
        app_commands.Choice(name="All time", value="all"),
        app_commands.Choice(name="Last 30 days", value="30d"),
        app_commands.Choice(name="Last 7 days", value="7d"),
        app_commands.Choice(name="Today", value="1d"),
    ])
    async def leaderboard(self, interaction: discord.Interaction, period: app_commands.Choice[str], limit: int = 10):
        await interaction.response.defer(thinking=False, ephemeral=True)
        now = utcnow()
        match period.value:
            case "all":
                since = None
            case "30d":
                since = now - timedelta(days=30)
            case "7d":
                since = now - timedelta(days=7)
            case "1d":
                since = now - timedelta(days=1)
            case _:
                since = None
        rows = await self.store.leaderboard(interaction.guild.id, since, limit)
        if not rows:
            await interaction.followup.send("No activity yet.")
            return
        lines = []
        for rank, row in enumerate(rows, start=1):
            uid = row["user_id"]
            cnt = row["cnt"]
            member = interaction.guild.get_member(uid)
            name = member.display_name if member else f"<@{uid}>"
            lines.append(f"**{rank}.** {name} — {cnt} reports")
        await interaction.followup.send("Leaderboard (" + period.name + ")\n\n" + "\n".join(lines))

    @ensure_access()
    @activity.command(name="inactive", description="List users with no activity since N days")
    async def inactive(self, interaction: discord.Interaction, days: app_commands.Range[int, 1, 365] = 30):
        await interaction.response.defer(thinking=False, ephemeral=True)
        cutoff = utcnow() - timedelta(days=days)
        rows = await self.store.inactive_since(interaction.guild.id, cutoff)
        if not rows:
            await interaction.followup.send(f"No users are inactive beyond {days} days.")
            return
        lines = []
        for row in rows:
            uid = row["user_id"]
            last_unix = row["last_ts_unix"]
            member = interaction.guild.get_member(uid)
            name = member.display_name if member else f"<@{uid}>"
            lines.append(f"{name} — last on <t:{last_unix}:F>")
        await interaction.followup.send("Inactive users:\n" + "\n".join(lines))

    @ensure_access()
    @activity.command(name="user", description="Show a user's recent activity")
    async def user(self, interaction: discord.Interaction, user: discord.User):
        await interaction.response.defer(thinking=False, ephemeral=True)
        rows = await self.store.user_activity(interaction.guild.id, user.id, None, limit=20)
        if not rows:
            await interaction.followup.send(f"No activity recorded for {user.mention}.")
            return
        lines = [f"Recent for {user.mention}:"]
        for row in rows:
            ts_unix = row["ts_unix"]
            op = row["op_name"]
            active = "Yes" if row["leading_ship"] else "No"
            lines.append(f"• <t:{ts_unix}:F> — {op} — Active Participant: {active}")
        await interaction.followup.send("\n".join(lines))

    @ensure_access()
    @activity.command(name="export", description="Export activity as CSV")
    @app_commands.describe(period="Optional time window to filter")
    @app_commands.choices(period=[
        app_commands.Choice(name="All time", value="all"),
        app_commands.Choice(name="Last 30 days", value="30d"),
        app_commands.Choice(name="Last 7 days", value="7d"),
        app_commands.Choice(name="Today", value="1d"),
    ])
    async def export(self, interaction: discord.Interaction, period: Optional[app_commands.Choice[str]] = None):
        await interaction.response.defer(thinking=True, ephemeral=True)
        now = utcnow()
        since: Optional[datetime] = None
        if period:
            if period.value == "30d":
                since = now - timedelta(days=30)
            elif period.value == "7d":
                since = now - timedelta(days=7)
            elif period.value == "1d":
                since = now - timedelta(days=1)
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["guild_id", "channel_id", "message_id", "line_no", "user_id", "reporter_id", "op_name", "active_participant", "ts_unix"])
        async for row in self.store.export_rows(interaction.guild.id, since):
            g, ch, mid, ln, uid, rid, op, leading, tsu = row
            writer.writerow([g, ch, mid, ln, uid, rid, op, 1 if leading else 0, tsu])
        data = buf.getvalue().encode("utf-8")
        file = discord.File(io.BytesIO(data), filename="activity_export.csv")
        await interaction.followup.send(content="Here is your CSV.", file=file)

    @app_commands.default_permissions(manage_guild=True)
    @activity.command(name="delete_entry", description="Remove a specific entry by message and line")
    async def delete_entry(self, interaction: discord.Interaction, message_id: str, line_no: int):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("Manage Server permission required.", ephemeral=True)
            return
        n = await self.store.delete_by_msg_line(interaction.guild.id, int(message_id), line_no)
        await interaction.response.send_message(f"Removed {n} entr{'y' if n==1 else 'ies'}.", ephemeral=True)

    @app_commands.default_permissions(manage_guild=True)
    @activity.command(name="delete_entries", description="Remove a range of entries by message and line range")
    async def delete_entries(self, interaction: discord.Interaction, message_id: str, start_line: int, end_line: int):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("Manage Server permission required.", ephemeral=True)
            return
        if start_line > end_line:
            start_line, end_line = end_line, start_line
        n = await self.store.delete_range_by_msg(interaction.guild.id, int(message_id), start_line, end_line)
        await interaction.response.send_message(f"Removed {n} entr{'y' if n==1 else 'ies'}.", ephemeral=True)

    @app_commands.default_permissions(manage_guild=True)
    @activity.command(name="delete_user", description="Remove all entries for a user in this server")
    async def delete_user(self, interaction: discord.Interaction, user: discord.User):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("Manage Server permission required.", ephemeral=True)
            return
        await self.store.delete_user_in_guild(interaction.guild.id, user.id)
        await interaction.response.send_message(f"All entries for {user.mention} removed in this server.", ephemeral=True)

    @app_commands.default_permissions(manage_guild=True)
    @activity.command(name="purge_all", description="Purge all activity logs in this server")
    async def purge_all(self, interaction: discord.Interaction, confirm: bool):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("Manage Server permission required.", ephemeral=True)
            return
        if not confirm:
            await interaction.response.send_message("Set confirm=true to proceed.", ephemeral=True)
            return
        await self.store.purge_guild(interaction.guild.id)
        await interaction.response.send_message("All activity logs in this server have been purged.", ephemeral=True)

    @app_commands.default_permissions(manage_guild=True)
    @activity.command(name="reindex", description="Recreate helpful indexes (safe to run any time)")
    async def reindex(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("Manage Server permission required.", ephemeral=True)
            return
        await self.bot.store._db.executescript(
            """
            CREATE INDEX IF NOT EXISTS idx_activities_user_ts ON activities(user_id, ts_unix);
            CREATE INDEX IF NOT EXISTS idx_activities_guild_ts ON activities(guild_id, ts_unix);
            CREATE INDEX IF NOT EXISTS idx_activities_guild_user ON activities(guild_id, user_id);
            CREATE INDEX IF NOT EXISTS idx_activities_guild_msg_line ON activities(guild_id, message_id, line_no);
            """
        )
        await self.bot.store._db.commit()
        await interaction.response.send_message("Indexes ensured.", ephemeral=True)

class ActivityBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix=commands.when_mentioned_or("!"), intents=INTENTS)
        self.store = ActivityStore(DB_PATH)

    async def setup_hook(self) -> None:
        await self.store.init()
        await self.add_cog(ActivityCog(self, self.store))
        if GUILD_ID:
            guild = discord.Object(id=GUILD_ID)
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
        else:
            await self.tree.sync()

    async def close(self) -> None:
        await self.store.close()
        await super().close()

def main():
    if not DISCORD_TOKEN:
        raise RuntimeError("DISCORD_TOKEN is not set")
    bot = ActivityBot()
    bot.run(DISCORD_TOKEN)

if __name__ == "__main__":
    main()