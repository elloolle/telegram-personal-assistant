import asyncio

from telethon import functions, types, utils


NAMES = {1: "В процессе 1", 2: "В процессе 2"}
CATEGORIES = ("contacts", "non_contacts", "groups", "broadcasts", "bots")


def has_categories(folder):
    return any(getattr(folder, name, False) for name in CATEGORIES)


def title_text(folder):
    title = getattr(folder, "title", "")
    return getattr(title, "text", title)


def same_peer(a, b):
    return utils.get_peer_id(a) == utils.get_peer_id(b)


def place_peer(folder, peer, include):
    """Keep unrelated peers, flags and pins intact."""
    folder.include_peers = [p for p in folder.include_peers if not same_peer(p, peer)]
    if include:
        folder.include_peers.append(peer)
        folder.exclude_peers = [p for p in folder.exclude_peers if not same_peer(p, peer)]
    else:
        folder.pinned_peers = [p for p in folder.pinned_peers if not same_peer(p, peer)]
        # Explicit exclusion also handles folders with a general contacts rule.
        if has_categories(folder) and not any(same_peer(p, peer) for p in folder.exclude_peers):
            folder.exclude_peers.append(peer)
    return folder


class Folders:
    def __init__(self, client):
        self.client = client
        self.lock = asyncio.Lock()

    async def move(self, peer, number):
        async with self.lock:
            response = await self.client(functions.messages.GetDialogFiltersRequest())
            filters = response.filters
            found = {}
            for number_, name in NAMES.items():
                matches = [f for f in filters if title_text(f) == name]
                if len(matches) > 1:
                    raise ValueError(f"Есть несколько папок «{name}». Оставьте одну с этим названием.")
                if matches:
                    if not isinstance(matches[0], types.DialogFilter):
                        raise ValueError(f"«{name}» должна быть обычной, не общей папкой.")
                    found[number_] = matches[0]
            destination = found.get(number)
            if destination is None:
                used = {getattr(f, "id", 0) for f in filters}
                folder_id = next(i for i in range(2, 256) if i not in used)
                destination = types.DialogFilter(
                    id=folder_id, title=types.TextWithEntities(NAMES[number], []),
                    pinned_peers=[], include_peers=[], exclude_peers=[])
            # First add to destination. If removal fails, retry converges safely.
            place_peer(destination, peer, True)
            await self.client(functions.messages.UpdateDialogFilterRequest(
                id=destination.id, filter=destination))
            other = found.get(3 - number)
            if other:
                place_peer(other, peer, False)
                # Telegram rejects a folder with no peers and no type rules
                # (FILTER_INCLUDE_EMPTY). Delete only that empty shell; no chat
                # or message is deleted. It will be recreated on the next move.
                empty = not other.include_peers and not other.pinned_peers and not has_categories(other)
                await self.client(functions.messages.UpdateDialogFilterRequest(
                    id=other.id, filter=None if empty else other))
