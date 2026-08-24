import json
import asyncio
import threading
from pathlib import Path
from typing import Optional, Set
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star
from astrbot.api import logger, AstrBotConfig
from astrbot.api.message_components import Node, Plain
from astrbot.core.utils.astrbot_path import get_astrbot_data_path

try:
    import aiosqlite
    HAS_AIOSQLITE = True
except ImportError:
    HAS_AIOSQLITE = False
    logger.warning("[UserAgreement] 未安装 aiosqlite，将使用同步SQLite")


class UserAgreement(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self._lock = asyncio.Lock()  # 异步锁
        self._thread_lock = threading.RLock()  # 线程锁（用于同步操作）
        self._executor = ThreadPoolExecutor(max_workers=2)  # 线程池
        self._consented_users: Set[str] = set()
        self._reminded_users: Set[str] = set()
        self._db_path = None
        self._conn = None  # 异步连接
        self._sync_conn = None  # 同步连接（用于线程池）
        
        # 初始化
        self._init_paths()
        
    def _init_paths(self):
        """初始化路径"""
        data_path = Path(get_astrbot_data_path()) / "plugin_data" / self.name
        data_path.mkdir(parents=True, exist_ok=True)
        self._db_path = data_path / "consents.db"
        self._json_backup = data_path / "consented_users_backup.json"

    async def _init_database(self):
        """异步初始化数据库"""
        if HAS_AIOSQLITE:
            try:
                self._conn = await aiosqlite.connect(str(self._db_path))
                await self._conn.execute("""
                    CREATE TABLE IF NOT EXISTS consents (
                        user_id TEXT PRIMARY KEY,
                        consented_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                await self._conn.execute("""
                    CREATE TABLE IF NOT EXISTS reminded_users (
                        user_id TEXT PRIMARY KEY,
                        reminded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                await self._conn.commit()
                logger.info("[UserAgreement] 异步数据库初始化成功")
                return
            except Exception as e:
                logger.error(f"[UserAgreement] 异步数据库初始化失败: {e}")
                self._conn = None
        
        # 回退到同步数据库
        await self._init_sync_database()

    async def _init_sync_database(self):
        """初始化同步数据库（在线程池中运行）"""
        def _init():
            import sqlite3
            self._sync_conn = sqlite3.connect(
                str(self._db_path), 
                check_same_thread=False,
                timeout=30
            )
            self._sync_conn.execute("""
                CREATE TABLE IF NOT EXISTS consents (
                    user_id TEXT PRIMARY KEY,
                    consented_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self._sync_conn.execute("""
                CREATE TABLE IF NOT EXISTS reminded_users (
                    user_id TEXT PRIMARY KEY,
                    reminded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self._sync_conn.commit()
        
        try:
            await asyncio.get_event_loop().run_in_executor(self._executor, _init)
            logger.info("[UserAgreement] 同步数据库初始化成功")
        except Exception as e:
            logger.error(f"[UserAgreement] 同步数据库初始化失败: {e}")

    async def _load_data(self):
        """异步加载数据"""
        # 尝试从数据库加载
        if self._conn:
            try:
                async with self._conn.execute("SELECT user_id FROM consents") as cursor:
                    rows = await cursor.fetchall()
                    self._consented_users = {row[0] for row in rows}
                logger.info(f"[UserAgreement] 从数据库加载 {len(self._consented_users)} 个用户")
                return
            except Exception as e:
                logger.error(f"[UserAgreement] 数据库加载失败: {e}")
        
        if self._sync_conn:
            try:
                def _load():
                    cursor = self._sync_conn.execute("SELECT user_id FROM consents")
                    return {row[0] for row in cursor.fetchall()}
                
                self._consented_users = await asyncio.get_event_loop().run_in_executor(
                    self._executor, _load
                )
                logger.info(f"[UserAgreement] 从同步数据库加载 {len(self._consented_users)} 个用户")
                return
            except Exception as e:
                logger.error(f"[UserAgreement] 同步数据库加载失败: {e}")
        
        # 从JSON备份加载
        if self._json_backup.exists():
            try:
                def _load_json():
                    return set(json.loads(self._json_backup.read_text(encoding="utf-8")))
                
                self._consented_users = await asyncio.get_event_loop().run_in_executor(
                    self._executor, _load_json
                )
                logger.info(f"[UserAgreement] 从JSON备份加载 {len(self._consented_users)} 个用户")
            except Exception as e:
                logger.error(f"[UserAgreement] JSON备份加载失败: {e}")

    async def _save_consent(self, user_id: str):
        """异步保存单个用户的同意"""
        async with self._lock:
            # 更新内存
            self._consented_users.add(user_id)
            
            # 保存到数据库
            if self._conn:
                try:
                    await self._conn.execute(
                        "INSERT OR IGNORE INTO consents (user_id) VALUES (?)",
                        (user_id,)
                    )
                    await self._conn.commit()
                except Exception as e:
                    logger.error(f"[UserAgreement] 数据库保存失败: {e}")
            
            elif self._sync_conn:
                def _save():
                    self._sync_conn.execute(
                        "INSERT OR IGNORE INTO consents (user_id) VALUES (?)",
                        (user_id,)
                    )
                    self._sync_conn.commit()
                
                try:
                    await asyncio.get_event_loop().run_in_executor(self._executor, _save)
                except Exception as e:
                    logger.error(f"[UserAgreement] 同步数据库保存失败: {e}")
            
            # 异步保存JSON备份（每10次保存一次，避免频繁IO）
            if len(self._consented_users) % 10 == 0:
                await self._save_json_backup()

    async def _save_json_backup(self):
        """异步保存JSON备份"""
        def _save():
            self._json_backup.write_text(
                json.dumps(list(self._consented_users), ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
        
        try:
            await asyncio.get_event_loop().run_in_executor(self._executor, _save)
        except Exception as e:
            logger.error(f"[UserAgreement] JSON备份保存失败: {e}")

    async def _clear_all_data(self):
        """异步清除所有数据"""
        async with self._lock:
            count = len(self._consented_users)
            
            # 清除内存
            self._consented_users.clear()
            self._reminded_users.clear()
            
            # 清除数据库
            if self._conn:
                try:
                    await self._conn.execute("DELETE FROM consents")
                    await self._conn.execute("DELETE FROM reminded_users")
                    await self._conn.commit()
                except Exception as e:
                    logger.error(f"[UserAgreement] 数据库清除失败: {e}")
            
            elif self._sync_conn:
                def _clear():
                    self._sync_conn.execute("DELETE FROM consents")
                    self._sync_conn.execute("DELETE FROM reminded_users")
                    self._sync_conn.commit()
                
                try:
                    await asyncio.get_event_loop().run_in_executor(self._executor, _clear)
                except Exception as e:
                    logger.error(f"[UserAgreement] 同步数据库清除失败: {e}")
            
            # 清除JSON备份
            await self._save_json_backup()
            
            return count

    def _is_admin(self, sender_id: str) -> bool:
        """检查是否为管理员"""
        admin_raw = self.config.get("admin_list", "")
        if not admin_raw:
            return False
        admins = [a.strip() for a in admin_raw.split("\n") if a.strip()]
        return sender_id in admins

    def _get_cmd(self, name: str, default: str = "") -> str:
        """获取配置"""
        return self.config.get(name, default)

    def _normalize_cmd(self, cmd: str) -> str:
        """标准化命令"""
        return " ".join(cmd.lower().split())

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_all_message(self, event: AstrMessageEvent, *args):
        """主事件处理"""
        try:
            # 获取事件和消息
            evt = args[0] if args else event
            
            # 安全获取发送者ID
            try:
                sender_id = str(evt.message_obj.sender.user_id)
            except (AttributeError, Exception):
                # 如果无法获取发送者，可能是系统消息
                return
            
            message_str = evt.message_str.strip()
            if not message_str:
                return
            
            normalized_msg = self._normalize_cmd(message_str)
            
            # 处理同意命令
            if await self._handle_agree(normalized_msg, sender_id, evt):
                evt.stop_event()
                return
            
            # 处理撤销命令
            if await self._handle_revoke(normalized_msg, sender_id, evt):
                evt.stop_event()
                return
            
            # 处理统计命令
            if await self._handle_stats(normalized_msg, sender_id, evt):
                evt.stop_event()
                return
            
            # 检查是否已同意
            if sender_id in self._consented_users:
                return
            
            # 处理提醒
            await self._handle_reminder(sender_id, evt)
            evt.stop_event()
            
        except Exception as e:
            logger.error(f"[UserAgreement] 处理消息出错: {e}", exc_info=True)

    async def _handle_agree(self, normalized_msg: str, sender_id: str, evt) -> bool:
        """处理同意命令"""
        agree_cmd = self._get_cmd("agree_command", "同意")
        
        # 构建同意命令列表
        agree_cmds = ["/同意", "/agree", f"/{agree_cmd}", agree_cmd]
        normalized_agree_cmds = {self._normalize_cmd(cmd) for cmd in agree_cmds}
        
        if normalized_msg in normalized_agree_cmds:
            if sender_id in self._consented_users:
                await evt.plain_result(
                    self._get_cmd("already_agreed_text", "您已同意过用户协议")
                )
            else:
                await self._save_consent(sender_id)
                logger.info(f"[UserAgreement] 用户 {sender_id} 已同意")
                await evt.plain_result(
                    self._get_cmd("agree_success_text", "✅ 已同意用户协议")
                )
            return True
        return False

    async def _handle_revoke(self, normalized_msg: str, sender_id: str, evt) -> bool:
        """处理撤销命令"""
        revoke_cmd = self._get_cmd("revoke_command", "撤销所有同意")
        revoke_cmds = ["/撤销所有同意", f"/{revoke_cmd}", revoke_cmd]
        normalized_revoke_cmds = {self._normalize_cmd(cmd) for cmd in revoke_cmds}
        
        if normalized_msg in normalized_revoke_cmds:
            if self._is_admin(sender_id):
                count = await self._clear_all_data()
                logger.info(f"[UserAgreement] 管理员 {sender_id} 撤销了 {count} 个同意")
                await evt.plain_result(
                    self._get_cmd("revoke_success_text", f"✅ 已撤销 {count} 个用户的同意")
                )
            else:
                await evt.plain_result(
                    self._get_cmd("revoke_no_admin_text", "❌ 无权限")
                )
            return True
        return False

    async def _handle_stats(self, normalized_msg: str, sender_id: str, evt) -> bool:
        """处理统计命令"""
        if normalized_msg == "/协议统计" and self._is_admin(sender_id):
            stats = f"📊 统计信息\n已同意用户: {len(self._consented_users)}\n待提醒用户: {len(self._reminded_users)}"
            await evt.plain_result(stats)
            return True
        return False

    async def _handle_reminder(self, sender_id: str, evt):
        """处理提醒"""
        if sender_id in self._reminded_users:
            await evt.plain_result(
                self._get_cmd("repeat_reminder_text", "请先同意用户协议")
            )
            return
        
        self._reminded_users.add(sender_id)
        
        # 获取协议内容
        ua_title = self._get_cmd("user_agreement_title", "用户协议")
        pp_title = self._get_cmd("privacy_policy_title", "隐私政策")
        
        ua_text = self._get_cmd("user_agreement", "暂无内容")
        pp_text = self._get_cmd("privacy_policy", "暂无内容")
        
        # 创建节点
        node_ua = Node(uin=sender_id, name=ua_title, content=[Plain(ua_text)])
        node_pp = Node(uin=sender_id, name=pp_title, content=[Plain(pp_text)])
        
        reminder_text = self._get_cmd(
            "reminder_text",
            "请先阅读并同意以下协议：\n发送 /同意 表示同意"
        )
        
        await evt.plain_result(reminder_text)
        await evt.chain_result([node_ua, node_pp])

    async def __aenter__(self):
        """异步上下文管理器入口"""
        await self._init_database()
        await self._load_data()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器退出"""
        if self._conn:
            await self._conn.close()
        if self._sync_conn:
            self._sync_conn.close()
        self._executor.shutdown(wait=False)