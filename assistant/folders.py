import asyncio

from telethon import functions, types, utils


NAMES = {2: "В процессе"}
CATEGORIES = ("contacts", "non_contacts", "groups", "broadcasts", "bots")


def has_categories(folder):
    return any(getattr(folder, name, False) for name in CATEGORIES)


def title_text(folder):
    title = getattr(folder, "title", "")
    return getattr(title, "text", title)


def same_peer(a, b):
    return utils.get_peer_id(a) == utils.get_peer_id(b)


def unique_peers(peers):
    result = []
    for peer in peers:
        if not any(same_peer(peer, previous) for previous in result):
            result.append(peer)
    return result


def place_peer(folder, peer, include):
    folder.include_peers = [p for p in folder.include_peers if not same_peer(p, peer)]
    if include:
        folder.include_peers.append(peer)
        folder.exclude_peers = [p for p in folder.exclude_peers if not same_peer(p, peer)]
    else:
        folder.pinned_peers = [p for p in folder.pinned_peers if not same_peer(p, peer)]
        if has_categories(folder) and not any(same_peer(p, peer) for p in folder.exclude_peers):
            folder.exclude_peers.append(peer)
    return folder


class Folders:
    def __init__(self, client):
        self.client = client
        self.lock = asyncio.Lock()

    async def _update(self, folder_id, folder):
        accepted = await self.client(functions.messages.UpdateDialogFilterRequest(id=folder_id, filter=folder))
        if accepted is False:
            raise ValueError('Telegram не подтвердил обновление папки.')

    async def normalize(self):
        async with self.lock:
            response = await self.client(functions.messages.GetDialogFiltersRequest())
            return await self._normalize(response.filters)

    async def _normalize(self, filters):
        found = {}
        for name in ("В процессе", "В процессе 1", "В процессе 2"):
            matches = [folder for folder in filters if title_text(folder) == name]
            if len(matches) > 1:
                raise ValueError(f"Есть несколько папок «{name}». Оставьте одну с этим названием.")
            if matches:
                if not isinstance(matches[0], types.DialogFilter):
                    raise ValueError(f"«{name}» должна быть обычной, не общей папкой.")
                found[name] = matches[0]
        destination = found.get("В процессе")
        legacy = found.get("В процессе 2")
        if legacy:
            if destination:
                destination.include_peers = unique_peers(destination.include_peers + legacy.include_peers)
                destination.pinned_peers = unique_peers(destination.pinned_peers + legacy.pinned_peers)
                destination.exclude_peers = [peer for peer in destination.exclude_peers
                                            if any(same_peer(peer, other) for other in legacy.exclude_peers)
                                            and not any(same_peer(peer, other) for other in destination.include_peers)]
                for field in CATEGORIES:
                    setattr(destination, field, bool(getattr(destination, field, False) or getattr(legacy, field, False)))
                for field in ("exclude_muted", "exclude_read", "exclude_archived"):
                    setattr(destination, field, bool(getattr(destination, field, False) and getattr(legacy, field, False)))
                await self._update(destination.id, destination)
                await self._update(legacy.id, None)
            else:
                destination = legacy
                destination.title = types.TextWithEntities("В процессе", [])
                await self._update(destination.id, destination)
        first = found.get("В процессе 1")
        if first:
            peers = unique_peers(first.include_peers + first.pinned_peers)
            for offset in range(0, len(peers), 100):
                accepted = await self.client(functions.folders.EditPeerFoldersRequest(folder_peers=[
                    types.InputFolderPeer(peer=peer, folder_id=1) for peer in peers[offset:offset + 100]]))
                if accepted is False:
                    raise ValueError('Telegram не подтвердил перенос чатов в архив.')
            await self._update(first.id, None)
        return destination

    async def move(self, peer, number):
        if number != 2:
            return
        async with self.lock:
            response = await self.client(functions.messages.GetDialogFiltersRequest())
            destination = await self._normalize(response.filters)
            if destination is None:
                used = {getattr(folder, "id", 0) for folder in response.filters}
                folder_id = next(i for i in range(2, 256) if i not in used)
                destination = types.DialogFilter(id=folder_id, title=types.TextWithEntities("В процессе", []),
                                                 pinned_peers=[], include_peers=[], exclude_peers=[])
            place_peer(destination, peer, True)
            await self._update(destination.id, destination)
