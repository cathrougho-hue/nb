#!/usr/bin/env python3
"""飞书案件管理机器人 v3 - 私聊主导 / 群聊需@ / 通知私发谢嘉敏"""

import json, time, requests, re, os, hashlib, difflib
from datetime import datetime

# ===== 配置 =====
APP_ID = os.environ.get("FEISHU_APP_ID", "")
APP_SECRET = os.environ.get("FEISHU_APP_SECRET", "")
APP_TOKEN = os.environ.get("FEISHU_APP_TOKEN", "")
TABLE_CIVIL = os.environ.get("TABLE_CIVIL", "tblB0SxAdxeRLOJR")
TABLE_CRIMINAL = os.environ.get("TABLE_CRIMINAL", "tbl3BokyyBYnvJAZ")

# 通知目标：谢嘉敏
NOTIFY_OPEN_ID = os.environ.get("NOTIFY_OPEN_ID", "")

STATE_FILE = os.path.expanduser("~/.case_bot_state.json")
BOT_APP_ID = os.environ.get("FEISHU_APP_ID", "")

# ===== 飞书 API =====
def get_token():
    r = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
        json={"app_id": APP_ID, "app_secret": APP_SECRET})
    return r.json()["tenant_access_token"]

def get_chats(token):
    r = requests.get("https://open.feishu.cn/open-apis/im/v1/chats?page_size=50",
        headers={"Authorization": f"Bearer {token}"})
    return {c["chat_id"]: c for c in r.json().get("data", {}).get("items", [])}

def get_messages(token, chat_id):
    url = f"https://open.feishu.cn/open-apis/im/v1/messages?container_id_type=chat&container_id={chat_id}&page_size=10&sort_type=ByCreateTimeDesc"
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    return r.json().get("data", {})

def send_to_chat(token, chat_id, text):
    body = {"receive_id": chat_id, "msg_type": "text", "content": json.dumps({"text": text})}
    r = requests.post("https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}, json=body)
    return r.json()

def reply_to(token, chat_id, is_group, text):
    """私聊用 send_to_user，群聊用 send_to_chat"""
    if is_group:
        return send_to_chat(token, chat_id, text)
    else:
        return send_to_user(token, NOTIFY_OPEN_ID, text)

def send_to_user(token, open_id, text):
    """私发消息给指定用户"""
    body = {"receive_id": open_id, "msg_type": "text", "content": json.dumps({"text": text})}
    r = requests.post("https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=open_id",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}, json=body)
    return r.json()

def add_record(token, table, fields):
    r = requests.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{table}/records",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}, json={"fields": fields})
    return r.json()

def get_all_records(token, table):
    r = requests.get(
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{table}/records?page_size=100",
        headers={"Authorization": f"Bearer {token}"})
    return r.json().get("data", {}).get("items", [])

def update_record(token, table, record_id, fields):
    r = requests.put(
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{table}/records/{record_id}",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}, json={"fields": fields})
    return r.json()

# ===== 案件名称模糊匹配 =====
_case_cache = {"time": 0, "cases": []}

def load_cases(token):
    now = time.time()
    if now - _case_cache["time"] < 30:
        return _case_cache["cases"]
    cases = []
    for table in [TABLE_CIVIL, TABLE_CRIMINAL]:
        for item in get_all_records(token, table):
            name = item["fields"].get("案件名称")
            if name:
                cases.append((name, table, item["record_id"], item["fields"]))
    _case_cache["time"] = now
    _case_cache["cases"] = cases
    return cases

def find_case(token, query):
    query = query.strip()
    cases = load_cases(token)
    if not cases: return None

    # 精确匹配
    for name, table, rid, fields in cases:
        if name == query: return (name, table, rid, fields)

    # 包含匹配
    matches = [(n, t, r, f) for n, t, r, f in cases if query in n]
    if len(matches) == 1: return matches[0]
    if len(matches) > 1:
        matches.sort(key=lambda x: len(x[0]))
        return matches[0]

    # 反向：查询词包含案件名关键词
    for name, table, rid, fields in cases:
        keywords = re.findall(r'[一-鿿]{2,}', name)
        for kw in keywords:
            if kw in query: return (name, table, rid, fields)

    # 模糊匹配
    names = [c[0] for c in cases]
    best = difflib.get_close_matches(query, names, n=1, cutoff=0.4)
    if best:
        for name, table, rid, fields in cases:
            if name == best[0]: return (name, table, rid, fields)

    return None

# ===== 自然语言解析 =====
def parse_natural(text, token):
    text = re.sub(r'@\S+', '', text).strip()
    if not text: return None

    if text in ("帮助", "help", "?", "怎么用"):
        return ("reply", HELP_TEXT)

    # 新增案件（详细）
    m = re.match(r'(?:新增|新建|添加|录入)(?:案件)?[：:\s]*(.+?)\s*\|\s*(.+?)\s*\|\s*(.+?)\s*\|\s*(.+?)\s*\|\s*(.+?)\s*\|\s*(.+)', text)
    if m:
        fields = {"案件名称": m.group(1).strip(), "案件类型": m.group(2).strip(),
                  "委托人": m.group(3).strip(), "对方当事人": m.group(4).strip(),
                  "承办人": m.group(5).strip(), "案由": m.group(6).strip()}
        table = TABLE_CRIMINAL if "刑事" in fields["案件类型"] else TABLE_CIVIL
        return ("add", {"table": table, "fields": fields},
                f"已录入「{fields['案件名称']}」({fields['案件类型']})")

    # 新增案件（快速）
    m = re.match(r'(?:新增|新建|添加)[：:\s]*(.+?)\s*\|\s*(.+)', text)
    if m:
        name, ctype = m.group(1).strip(), m.group(2).strip()
        table = TABLE_CRIMINAL if "刑事" in ctype else TABLE_CIVIL
        return ("add", {"table": table, "fields": {"案件名称": name, "案件类型": ctype}},
                f"已录入「{name}」")

    # 查询
    for pat in [r'(.+?)(?:现在)?(?:什么|啥|怎么样|如何)(?:状态|进度|情况)',
                r'(?:查|查一下|看看)(?:案件)?[：:\s]*(.+)',
                r'(?:查询|查找|搜索)[：:\s]*(.+)']:
        m = re.match(pat, text)
        if m:
            case = find_case(token, m.group(1).strip())
            if case:
                f = case[3]
                return ("reply",
                    f"「{case[0]}」\n状态：{f.get('案件状态','未设置')}\n"
                    f"类型：{f.get('案件类型','?')}\n承办人：{f.get('承办人','?')}\n"
                    f"下步工作：{f.get('下一步工作安排','无')}\n"
                    f"开庭日期：{f.get('开庭日期','未设置')}")
            return ("reply", f"没找到和「{m.group(1).strip()}」相关的案件")

    # 状态更新（自然语言）
    status_kw = {
        "已结案":"已结案","结案了":"已结案","结了":"已结案","已结":"已结案","结案":"已结案",
        "已调解":"已调解","调解了":"已调解","调解结案":"已调解",
        "已归档":"待归档","归档了":"待归档","归档":"待归档",
        "已判决":"已判决/已裁决","判了":"已判决/已裁决","判决了":"已判决/已裁决",
        "已立案":"已立案","立案了":"已立案",
        "强制执行":"强制执行","执行阶段":"强制执行",
        "在办":"在办","进行中":"在办",
        "撤诉":"已结案","撤诉了":"已结案","驳回":"已结案",
        "二审":"二审","上诉了":"二审",
    }
    for kw, status in status_kw.items():
        m = re.match(rf'(.+?)(?:的案子|的案件|案|案件)?(?:已经|已|应该)?{kw}', text)
        if m:
            query = m.group(1).strip()
            if not query or len(query) < 1: continue
            case = find_case(token, query)
            if case:
                return ("update", {
                    "table": case[1], "rid": case[2],
                    "fields": {"案件状态": status},
                    "name": case[0], "new_status": status
                }, f"确认将「{case[0]}」状态改为「{status}」？\n回复「是」确认，「否」取消")
            return ("reply", f"没找到和「{query}」相关的案件，请说全案件名称")

    # 推进
    m = re.match(r'(.+?)(?:案件|的案子|案)?(?:推进|进展|更新|跟进)[：:\s]*(.+)', text)
    if m:
        case = find_case(token, m.group(1).strip())
        if case:
            return ("update", {"table": case[1], "rid": case[2],
                    "fields": {"下一步工作安排": m.group(2).strip()}},
                    f"已更新「{case[0]}」推进事项")

    # 清单
    if text in ("清单", "列表", "所有案件", "案件列表"):
        cases = load_cases(token)
        if not cases: return ("reply", "暂无案件")
        civil = [c for c in cases if c[1] == TABLE_CIVIL]
        crim = [c for c in cases if c[1] == TABLE_CRIMINAL]
        lines = []
        if civil: lines.append(f"【民事/仲裁】{len(civil)}件")
        if crim: lines.append(f"【刑事】{len(crim)}件")
        lines.append(f"合计 {len(cases)} 件")
        return ("reply", "\n".join(lines))

    # 最近
    if text in ("最近", "最近案件", "最新"):
        cases = load_cases(token)
        if not cases: return ("reply", "暂无案件")
        recent = cases[-5:]
        lines = ["最近录入的案件："]
        for name, table, rid, fields in reversed(recent):
            s = fields.get("案件状态", "?")
            lines.append(f"• {name} [{s}]")
        return ("reply", "\n".join(lines))

    return None

HELP_TEXT = (
    "我是案件助手，你可以这样和我说：\n\n"
    "查询类：\n"
    "• 张三那个案子什么状态？\n"
    "• 查一下李四合同纠纷\n"
    "• 清单（看案件统计）\n\n"
    "更新类：\n"
    "• 张三的案子已经结案了\n"
    "• 东方那个案件已经调解\n"
    "• 李四案推进了，已联系法官\n\n"
    "录入类：\n"
    "• 新增案件 名称 | 类型 | 委托人 | 对方 | 承办人 | 案由\n"
    "• 新增 案件名称 | 类型"
)

# ===== 待确认的操作 =====
_pending = {}  # {chat_id: (action_type, data)}

# ===== 表格变更检测（仅私发谢嘉敏）=====
def table_snapshot(token):
    snap = {}
    for table in [TABLE_CIVIL, TABLE_CRIMINAL]:
        for item in get_all_records(token, table):
            rid = item["record_id"]
            fields = item.get("fields", {})
            h = hashlib.md5(json.dumps(fields, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
            snap[rid] = {"hash": h, "name": fields.get("案件名称", "?")}
    return snap

# ===== 主循环 =====
def main():
    token = get_token()
    print("案件机器人 v3 启动 (通知目标: 谢嘉敏)")

    state = {"last_msg_ids": {}}
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            state = json.load(f)
    last_msg_ids = state.get("last_msg_ids", {})

    old_snap = table_snapshot(token)
    last_check = time.time()

    while True:
        try:
            now = time.time()

            # ==== 消息处理 ====
            chats = get_chats(token)
            # 确保谢嘉敏私聊被监听
            P2P_ID = "oc_8ad7df8fac76df575535804d38cd69ca"
            if P2P_ID not in chats:
                chats[P2P_ID] = {"name": "谢嘉敏", "chat_type": "p2p"}

            for chat_id, chat_info in chats.items():
                chat_name = chat_info.get("name", "(私聊)")
                is_group = chat_info.get("chat_type") == "group_chat" or "邦信" in chat_name

                msgs = get_messages(token, chat_id)
                last_id = last_msg_ids.get(chat_id, "")

                for msg in msgs.get("items", []):
                    msg_id = msg["message_id"]
                    if last_id and msg_id <= last_id:
                        continue
                    if not last_id or msg_id > last_id:
                        last_msg_ids[chat_id] = msg_id
                    if msg["msg_type"] != "text":
                        continue
                    # 跳过自己的消息
                    if msg.get("sender", {}).get("id") == BOT_APP_ID:
                        continue

                    try:
                        text = json.loads(msg["body"]["content"]).get("text", "")
                    except:
                        continue
                    if not text:
                        continue

                    # 群聊需 @，私聊直接响应
                    if is_group and "@" not in text:
                        continue

                    print(f"[{chat_name}] {text[:60]}")

                    # 检查待确认
                    pending = _pending.get(chat_id)
                    if pending and text.strip() in ("是", "确认", "好的", "可以", "yes", "ok", "y"):
                        p_action, p_data = pending
                        if p_action == "update":
                            r = update_record(token, p_data["table"], p_data["rid"], p_data["fields"])
                            reply = f"已修改「{p_data['name']}」" if r["code"] == 0 else f"失败: {r.get('msg','?')}"
                        elif p_action == "add":
                            r = add_record(token, p_data["table"], p_data["fields"])
                            reply = f"已录入「{p_data['fields']['案件名称']}」" if r["code"] == 0 else f"失败: {r.get('msg','?')}"
                        else:
                            reply = "已取消"
                        _pending.pop(chat_id, None)
                        reply_to(token, chat_id, is_group,reply)
                        continue

                    if pending and text.strip() in ("否", "取消", "不要", "no", "n"):
                        _pending.pop(chat_id, None)
                        reply_to(token, chat_id, is_group,"已取消")
                        continue

                    # 解析命令
                    result = parse_natural(text, token)
                    print(f"  -> {result[0] if result else 'NO MATCH'}")
                    if not result:
                        continue

                    if len(result) == 3:
                        action, data, reply = result
                    else:
                        action, data = result
                        reply = data if action == "reply" else data

                    if action == "add":
                        r = add_record(token, data["table"], data["fields"])
                        reply = f"已录入「{data['fields']['案件名称']}」" if r["code"] == 0 else f"录入失败: {r.get('msg','?')}"

                    elif action == "update":
                        _pending[chat_id] = ("update", data)
                        # reply already contains the confirmation message from parse_natural
                        reply_to(token, chat_id, is_group,reply)
                        continue

                    if is_group:
                        reply_to(token, chat_id, is_group,reply)
                    else:
                        send_to_user(token, NOTIFY_OPEN_ID, reply)

            time.sleep(3)

            # ==== 表格变更检测（60秒，仅私发谢嘉敏）====
            if now - last_check > 60:
                new_snap = table_snapshot(token)
                changes = []
                for rid, info in new_snap.items():
                    if rid not in old_snap:
                        changes.append(f"新增「{info['name']}」")
                    elif info["hash"] != old_snap.get(rid, {}).get("hash", ""):
                        changes.append(f"修改「{info['name']}」")
                for rid in old_snap:
                    if rid not in new_snap:
                        changes.append(f"删除「{old_snap[rid]['name']}」")
                if changes:
                    msg = "表格变更：\n" + "\n".join(f"• {c}" for c in changes)
                    print(msg)
                    send_to_user(token, NOTIFY_OPEN_ID, msg)
                old_snap = new_snap
                last_check = now
                _case_cache["time"] = 0

            if int(now) % 30 == 0:
                state["last_msg_ids"] = last_msg_ids
                with open(STATE_FILE, "w") as f:
                    json.dump(state, f)

        except KeyboardInterrupt:
            print("\n停止")
            break
        except Exception as e:
            print(f"错误: {e}")
            time.sleep(10)
            try: token = get_token()
            except: pass

if __name__ == "__main__":
    main()
