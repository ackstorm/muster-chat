"""Address shape, reference resolution, and the one ACL predicate. Pure — no I/O.
Spec v2 §6 (identity), §6.1 (references), §9 (ACL)."""
from dataclasses import dataclass


class AddressError(ValueError):
    pass


@dataclass(frozen=True)
class Address:
    user: str
    host: str
    runtime: str
    project: str
    session: str

    def __str__(self) -> str:
        return "/".join((self.user, self.host, self.runtime, self.project, self.session))


def parse_agent_header(user: str, header: str) -> Address:
    """x-muster-agent = host/runtime/project/session. `user` comes from the resolver,
    never from the client (server-stamped, spec §6)."""
    parts = header.split("/")
    if len(parts) != 4 or not all(parts):
        raise AddressError("x-muster-agent must be host/runtime/project/session, all segments non-empty")
    return Address(user, *parts)


def matches(ref: str, addr: str) -> bool:
    """Shortest-unique-reference contract (§6.1): a ref matches iff it equals a
    contiguous '/'-joined slice of the address segments. No substring matching."""
    if not ref:
        return False
    segs = addr.split("/")
    return any("/".join(segs[i:j]) == ref for i in range(len(segs)) for j in range(i + 1, len(segs) + 1))


def visible(caller_user: str, caller_groups, agent_user: str, agent_groups) -> bool:
    """THE ACL predicate (§9). The only authorization rule in muster."""
    return caller_user == agent_user or bool(set(caller_groups) & set(agent_groups))
