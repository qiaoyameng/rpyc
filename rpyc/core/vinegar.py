"""Safe serialization for remote exceptions."""

import sys
import traceback

try:
    import exceptions as exceptions_module
except ImportError:
    import builtins as exceptions_module

try:
    from types import InstanceType, ClassType
except ImportError:
    InstanceType = None
    ClassType = type

from rpyc.core import brine
from rpyc.core import consts
from rpyc import version


REMOTE_LINE_START = "\n\n========= Remote Traceback "
REMOTE_LINE_END = " =========\n"
REMOTE_LINE = "{0}({{}}){1}".format(REMOTE_LINE_START, REMOTE_LINE_END)

STRUCTURED_EXCEPTION_MAGIC = "\x00rpyc-structured-exception"
STRUCTURED_EXCEPTION_VERSION = 1

MAX_EXCEPTION_NODES = 1000
MAX_EXCEPTION_DEPTH = 100
MAX_TEXT_LENGTH = 10_000
MAX_TOTAL_TEXT = 100_000
MAX_BYTES_LENGTH = 10_000
MAX_TOTAL_BYTES = 100_000
MAX_TOTAL_INTEGER_DIGITS = 100_000
MAX_COLLECTION_ITEMS = 1000
MAX_TOTAL_DATA_ITEMS = 100_000
MAX_DATA_DEPTH = 20
MAX_EXCEPTION_ARGS = 100
MAX_EXCEPTION_NOTES = 100
MAX_EXCEPTION_CHILDREN = 1000
MAX_TRACEBACK_FRAMES = 100
MAX_INTEGER_BITS = 1024

TRUNCATED = "truncated"
NODE = "node"
REFERENCE = "ref"

_GROUP_TYPE_NAMES = frozenset(("ExceptionGroup", "BaseExceptionGroup"))


def _group_base_class(typ):
    if typ is None:
        return None
    for name in _GROUP_TYPE_NAMES:
        group_cls = getattr(exceptions_module, name, None)
        if group_cls is not None and isinstance(typ, type) and issubclass(typ, group_cls):
            return group_cls
    return None


def _group_exceptions(exc):
    group_cls = _group_base_class(type(exc))
    if group_cls is None or not isinstance(exc, group_cls):
        return None
    try:
        children = group_cls.exceptions.__get__(exc)
        return tuple(children)
    except Exception:
        return ()


def _raw_args(exc):
    return _builtin_slot(BaseException.args, exc, ())


def _builtin_slot(descriptor, exc, default):
    try:
        value = descriptor.__get__(exc)
    except Exception:
        return default
    return value


def _raw_cause(exc):
    return _builtin_slot(BaseException.__cause__, exc, None)


def _raw_context(exc):
    return _builtin_slot(BaseException.__context__, exc, None)


def _raw_suppress_context(exc):
    return bool(_builtin_slot(BaseException.__suppress_context__, exc, False))


def _exception_message(exc, budget):
    group_cls = _group_base_class(type(exc))
    if group_cls is not None and isinstance(exc, group_cls):
        try:
            message = group_cls.message.__get__(exc)
            return _truncate_text(message if type(message) is str else str(message), budget)
        except Exception:
            return _truncate_text("<unprintable exception>", budget)

    args = _raw_args(exc)
    if len(args) == 0:
        return ""
    if len(args) == 1 and type(args[0]) is str:
        return _truncate_text(args[0], budget)
    return _truncate_text(f"<{len(args)} exception arguments>", budget)


class _TextBudget(object):
    def __init__(self, limit):
        self.remaining = limit
        self.bytes_remaining = MAX_TOTAL_BYTES
        self.integer_digits_remaining = MAX_TOTAL_INTEGER_DIGITS
        self.data_items_remaining = MAX_TOTAL_DATA_ITEMS

    def take_data_item(self):
        if self.data_items_remaining <= 0:
            return False
        self.data_items_remaining -= 1
        return True

    def take_bytes(self, size):
        allowed = min(size, max(0, self.bytes_remaining))
        self.bytes_remaining -= allowed
        return allowed

    def take_integer_digits(self, size):
        if size <= self.integer_digits_remaining:
            self.integer_digits_remaining -= size
            return size
        return 0


def _truncate_text(text, budget, max_length=MAX_TEXT_LENGTH):
    if type(text) is not str:
        text = str(text)
    data = text.encode("utf-8", "replace")
    limit = min(max_length, max(0, budget.remaining))
    if len(data) <= limit:
        budget.remaining -= len(data)
        return text
    marker = b"... [truncated]"
    result = data[:max(0, limit - len(marker))] + marker
    budget.remaining -= min(len(result), budget.remaining)
    return result.decode("utf-8", "replace")


def _safe_type_name(typ):
    try:
        module = typ.__module__
        name = typ.__name__
        module = module if type(module) is str else "unknown"
        name = name if type(name) is str else "unknown"
        return module, name
    except Exception:
        return "unknown", "unknown"


def _safe_exception_text(exc, budget):
    return _exception_message(exc, budget)


def _safe_data(obj, budget, depth=0):
    if depth > MAX_DATA_DEPTH:
        return _truncate_text("<data depth limit reached>", budget)

    obj_type = type(obj)
    if obj is None or obj is NotImplemented or obj is Ellipsis or obj_type is bool:
        return obj
    if obj_type is int:
        digit_count = len(str(abs(obj)))
        if abs(obj).bit_length() <= MAX_INTEGER_BITS and \
                budget.take_integer_digits(digit_count) == digit_count:
            return obj
        return _truncate_text("<integer too large>", budget)
    if obj_type is float or obj_type is complex:
        return obj
    if obj_type is str:
        return _truncate_text(obj, budget)
    if obj_type is bytes:
        allowed = budget.take_bytes(min(len(obj), MAX_BYTES_LENGTH))
        if allowed == len(obj):
            return obj
        return obj[:allowed] + b"... [truncated]"
    if obj_type is tuple or obj_type is frozenset:
        items = []
        for index, item in enumerate(obj):
            if index >= MAX_COLLECTION_ITEMS:
                items.append(_truncate_text("<collection truncated>", budget))
                break
            if not budget.take_data_item():
                items.append(_truncate_text("<data item limit reached>", budget))
                break
            items.append(_safe_data(item, budget, depth + 1))
        return tuple(items)
    if obj_type is slice:
        return slice(
            _safe_data(obj.start, budget, depth + 1),
            _safe_data(obj.stop, budget, depth + 1),
            _safe_data(obj.step, budget, depth + 1),
        )
    try:
        description = f"<non-serializable {type(obj).__module__}.{type(obj).__name__}>"
    except Exception:
        description = "<non-serializable object>"
    return _truncate_text(description, budget)


def _safe_args(exc, budget):
    if _group_exceptions(exc) is not None:
        return ()
    args = _raw_args(exc)
    result = [_safe_data(arg, budget) for arg in args[:MAX_EXCEPTION_ARGS]]
    if len(args) > MAX_EXCEPTION_ARGS:
        result.append(_truncate_text("<exception arguments truncated>", budget))
    return tuple(result)


def _safe_notes(exc, budget):
    try:
        notes = object.__getattribute__(exc, "__dict__").get("__notes__")
    except AttributeError:
        notes = None
    if type(notes) is not list and type(notes) is not tuple:
        return ()
    notes = tuple(notes)
    result = [
        _truncate_text(note, budget)
        for note in notes[:MAX_EXCEPTION_NOTES]
        if type(note) is str
    ]
    if len(notes) > MAX_EXCEPTION_NOTES:
        result.append(_truncate_text("<exception notes truncated>", budget))
    return tuple(result)


def _safe_attrs(exc, budget, include_local_version):
    remote_version = version.__version__ if include_local_version else "<version denied>"
    return (("_remote_version", _truncate_text(remote_version, budget)),)


def _traceback_text(identity, message, tb, include_local_traceback, budget):
    if not include_local_traceback:
        return "<traceback denied>"
    result = []

    def append_text(text):
        if budget.remaining > 0:
            result.append(_truncate_text(text, budget))

    append_text("Traceback (most recent call last):\n")
    if tb is not None:
        try:
            frames = traceback.StackSummary.extract(
                traceback.walk_tb(tb), limit=MAX_TRACEBACK_FRAMES, lookup_lines=True,
            )
        except Exception:
            frames = ()
        for frame in frames:
            filename = _truncate_text(getattr(frame, "filename", "<unknown>"), budget)
            name = _truncate_text(getattr(frame, "name", "<unknown>"), budget)
            line = _truncate_text(getattr(frame, "line", "") or "", budget)
            lineno = getattr(frame, "lineno", 0)
            append_text(f'  File "{filename}", line {lineno}, in {name}\n')
            append_text(f"    {line}\n")
    module, typname = identity
    append_text(f"{module}.{typname}: {message}\n")
    return "".join(result)


def _legacy_traceback_text(typ, val, tb, include_local_traceback):
    if include_local_traceback:
        return "".join(traceback.format_exception(typ, val, tb))
    return "<traceback denied>"


class _StructuredDumpState(object):
    def __init__(self, typ, val, tb, include_local_traceback, include_local_version,
                 max_nodes, max_depth, total_text_budget):
        self.val = val
        self.tb = tb
        self.include_local_traceback = include_local_traceback
        self.include_local_version = include_local_version
        self.max_nodes = max_nodes
        self.max_depth = max_depth
        self.budget = _TextBudget(total_text_budget)
        self.ids = {}
        self.nodes = []
        self.queue = []
        self.unreserved_ids = {}

    def reserve(self, exc, depth):
        existing = self.ids.get(id(exc))
        if existing is not None:
            return existing, False
        node_id = len(self.nodes)
        self.ids[id(exc)] = node_id
        self.unreserved_ids[node_id] = id(exc)
        self.nodes.append([node_id, None, None, None, None, None, None, None, None, False, ""])
        self.queue.append((node_id, exc, depth))
        return node_id, True

    def edge(self, exc, depth, hierarchy=False):
        if hierarchy and depth >= self.max_depth:
            return (TRUNCATED, "depth-limit")
        if not isinstance(exc, BaseException):
            return (TRUNCATED, "invalid-exception")
        if self.val is not exc and id(exc) not in self.ids and len(self.nodes) >= self.max_nodes:
            return (TRUNCATED, "node-limit")
        node_id, created = self.reserve(exc, depth)
        return (NODE if created else REFERENCE, node_id)

    def populate(self, node_id, exc, depth):
        module, name = _safe_type_name(type(exc))
        children = _group_exceptions(exc)
        cause = _raw_cause(exc)
        context = _raw_context(exc)
        suppress_context = _raw_suppress_context(exc)
        node = self.nodes[node_id]
        node[1] = (module, name)
        node[2] = _safe_exception_text(exc, self.budget)
        node[3] = _safe_args(exc, self.budget)
        node[4] = _safe_attrs(exc, self.budget, self.include_local_version)
        node[5] = _safe_notes(exc, self.budget)
        if children is None:
            node[6] = ()
        else:
            edges = tuple(
                self.edge(child, depth + 1, hierarchy=True)
                for child in children[:MAX_EXCEPTION_CHILDREN]
            )
            if len(children) > MAX_EXCEPTION_CHILDREN:
                edges += ((TRUNCATED, "children-limit"),)
            node[6] = edges
        node[7] = None if cause is None else self.edge(cause, 0)
        node[8] = None if context is None else self.edge(context, 0)
        node[9] = suppress_context
        if node_id == 0:
            node[10] = _traceback_text((module, name), node[2], self.tb,
                                       self.include_local_traceback, self.budget)

    def dump(self):
        root_id, _ = self.reserve(self.val, 0)
        while self.queue:
            node_id, exc, depth = self.queue.pop(0)
            if self.unreserved_ids.pop(node_id, None) != id(exc):
                self.nodes[node_id][6] = ((TRUNCATED, "reused-object-id"),)
                self.nodes[node_id][7] = None
                self.nodes[node_id][8] = None
                continue
            if isinstance(exc, BaseException):
                self.populate(node_id, exc, depth)
            else:
                self.nodes[node_id][6] = ((TRUNCATED, "invalid-exception"),)
        return (
            STRUCTURED_EXCEPTION_MAGIC,
            STRUCTURED_EXCEPTION_VERSION,
            root_id,
            tuple(tuple(part) for part in self.nodes),
        )


def dump_structured(typ, val, tb, include_local_traceback, include_local_version,
                    max_nodes=MAX_EXCEPTION_NODES, max_depth=MAX_EXCEPTION_DEPTH,
                    total_text_budget=MAX_TOTAL_TEXT):
    if not isinstance(val, BaseException):
        return dump_legacy(typ, val, tb, include_local_traceback, include_local_version)
    return _StructuredDumpState(
        typ, val, tb, include_local_traceback, include_local_version,
        max_nodes, max_depth, total_text_budget,
    ).dump()


def dump_legacy(typ, val, tb, include_local_traceback, include_local_version):
    if typ is StopIteration:
        return consts.EXC_STOP_ITERATION
    if type(typ) is str:
        return typ

    tbtext = _legacy_traceback_text(typ, val, tb, include_local_traceback)

    attrs = []
    args = []
    ignored_attrs = frozenset(("_remote_tb", "with_traceback"))
    for name in dir(val):
        if name == "args":
            for arg in val.args:
                args.append(arg if brine.dumpable(arg) else repr(arg))
        elif name.startswith("_") or name in ignored_attrs:
            continue
        else:
            try:
                attrval = getattr(val, name)
            except AttributeError:
                continue
            if not brine.dumpable(attrval):
                attrval = repr(attrval)
            attrs.append((name, attrval))

    remote_version = version.__version__ if include_local_version else "<version denied>"
    attrs.append(("_remote_version", remote_version))
    return (typ.__module__, typ.__name__), tuple(args), tuple(attrs), tbtext


def dump(typ, val, tb, include_local_traceback, include_local_version, structured=False):
    if not structured:
        return dump_legacy(typ, val, tb, include_local_traceback, include_local_version)
    try:
        return dump_structured(typ, val, tb, include_local_traceback, include_local_version)
    except Exception:
        return dump_legacy(typ, val, tb, include_local_traceback, include_local_version)


def _is_structured(val):
    return (type(val) is tuple and len(val) == 4 and val[0] == STRUCTURED_EXCEPTION_MAGIC)


def _resolve_exception_class(modname, clsname, import_custom_exceptions,
                             instantiate_custom_exceptions):
    if import_custom_exceptions and modname not in sys.modules:
        try:
            __import__(modname, None, None, "*")
        except Exception:
            pass

    if instantiate_custom_exceptions and not (
        modname == exceptions_module.__name__ and clsname in _GROUP_TYPE_NAMES
    ):
        module = sys.modules.get(modname) if modname in sys.modules else None
        cls = getattr(module, clsname, None)
    elif modname == exceptions_module.__name__ and clsname not in _GROUP_TYPE_NAMES:
        cls = getattr(exceptions_module, clsname, None)
    else:
        cls = None

    if not isinstance(cls, type) or not issubclass(cls, BaseException):
        return None
    return cls


class GenericException(Exception):
    """Raised when a remote exception cannot be instantiated locally."""


class RemoteExceptionReference(GenericException):
    """A hierarchy edge that points back to another remote exception node."""


class TruncatedRemoteException(GenericException):
    """A hierarchy edge explicitly cut off by the serializer."""


_generic_exceptions_cache = {}
_exception_classes_cache = {}


def _generic_exception_class(modname, clsname):
    fullname = str(f"{modname}.{clsname}")
    cls = _generic_exceptions_cache.get(fullname)
    if cls is None:
        fakemodule = {"__module__": f"{__name__}/{modname}"}
        cls = type(fullname, (GenericException,), fakemodule)
        _generic_exceptions_cache[fullname] = cls
    return cls


def _get_exception_class(cls):
    cached = _exception_classes_cache.get(cls)
    if cached is not None:
        return cached

    class Derived(cls):
        def __str__(self):
            try:
                text = cls.__str__(self)
            except Exception:
                text = "<Unprintable exception>"
            if getattr(self, "_remote_tb", ""):
                text += REMOTE_LINE.format(self._remote_tb.count(REMOTE_LINE_START) + 1)
                text += self._remote_tb
            return text

        def __repr__(self):
            return str(self)

    Derived.__name__ = cls.__name__
    Derived.__module__ = cls.__module__
    _exception_classes_cache[cls] = Derived
    return Derived


def _instantiate(cls):
    cls = _get_exception_class(cls)
    if ClassType is not type and isinstance(cls, ClassType):
        return InstanceType(cls)
    return cls.__new__(cls)


def _safe_tuple(value):
    return value if type(value) is tuple else ()


class _StructuredLoader(object):
    def __init__(self, payload, import_custom_exceptions, instantiate_custom_exceptions,
                 instantiate_oldstyle_exceptions):
        self.import_custom_exceptions = import_custom_exceptions
        self.instantiate_custom_exceptions = instantiate_custom_exceptions
        self.instantiate_oldstyle_exceptions = instantiate_oldstyle_exceptions
        self.raw_nodes = self._parse_nodes(payload[3])
        root_id = payload[2] if type(payload[2]) is int else 0
        self.root_id = root_id if root_id in self.raw_nodes else None
        self.objects = {}
        self.active_groups = set()
        self.text_budget = _TextBudget(MAX_TOTAL_TEXT)

    @staticmethod
    def _parse_nodes(raw_nodes):
        result = {}
        if type(raw_nodes) is not tuple:
            return result
        for index, node in enumerate(raw_nodes):
            if index >= MAX_EXCEPTION_NODES:
                break
            if type(node) is tuple and len(node) == 11:
                result[index] = node
        return result

    @staticmethod
    def _identity(node):
        identity = node[1]
        if type(identity) is tuple and len(identity) == 2:
            module = identity[0] if type(identity[0]) is str else "unknown"
            name = identity[1] if type(identity[1]) is str else "unknown"
            return module, name
        return "unknown", "unknown"

    def _native_group_class(self, node):
        module, name = self._identity(node)
        if module != exceptions_module.__name__ or name not in _GROUP_TYPE_NAMES:
            return None
        return getattr(exceptions_module, name, None)

    @staticmethod
    def _set_args(exc, args):
        try:
            exc.args = args
        except Exception:
            pass

    def _bounded_data(self, value, depth=0):
        if depth > MAX_DATA_DEPTH:
            return self._truncated("data-depth-limit")
        value_type = type(value)
        if value is None or value is NotImplemented or value is Ellipsis or value_type is bool:
            return value
        if value_type is int:
            digit_count = len(str(abs(value)))
            if abs(value).bit_length() > MAX_INTEGER_BITS or \
                    self.text_budget.integer_digits_remaining < digit_count:
                return self._bounded_text("<integer too large>")
            self.text_budget.integer_digits_remaining -= digit_count
            return value
        if value_type is float or value_type is complex:
            return value
        if value_type is str:
            return self._bounded_text(value)
        if value_type is bytes:
            size = min(len(value), MAX_BYTES_LENGTH, self.text_budget.bytes_remaining)
            self.text_budget.bytes_remaining -= size
            return value[:size] + (b"... [truncated]" if size < len(value) else b"")
        if value_type is tuple or value_type is frozenset:
            if not self.text_budget.take_data_item():
                return self._bounded_text("<data item limit reached>")
            items = tuple(self._bounded_data(item, depth + 1)
                          for item in tuple(value)[:MAX_COLLECTION_ITEMS])
            if len(value) > MAX_COLLECTION_ITEMS:
                items += (self._bounded_text("<collection truncated>"),)
            return items
        if value_type is slice:
            return slice(self._bounded_data(value.start, depth + 1),
                         self._bounded_data(value.stop, depth + 1),
                         self._bounded_data(value.step, depth + 1))
        return self._bounded_text("<non-serializable object>")

    @staticmethod
    def _add_notes(exc, notes):
        valid_notes = tuple(note for note in notes if type(note) is str)
        if not valid_notes:
            return
        add_note = getattr(exc, "add_note", None)
        if callable(add_note):
            for note in valid_notes:
                try:
                    add_note(note)
                except Exception:
                    pass
            return
        for note in notes:
            if type(note) is str:
                try:
                    existing_notes = list(getattr(exc, "__notes__", ()))
                    setattr(exc, "__notes__", tuple(existing_notes + [note]))
                except Exception:
                    pass

    def _bounded_notes(self, notes):
        if type(notes) is not tuple:
            return ()
        return tuple(self._bounded_text(note)
                     for note in notes[:MAX_EXCEPTION_NOTES]
                     if type(note) is str)

    @staticmethod
    def _set_attrs(exc, attrs):
        if type(attrs) is not tuple:
            return
        for attr in attrs:
            if type(attr) is tuple and len(attr) == 2 and type(attr[0]) is str:
                try:
                    setattr(exc, attr[0], attr[1])
                except Exception:
                    pass

    def _truncated(self, reason):
        reason = reason if type(reason) is str else "malformed-exception"
        return TruncatedRemoteException(f"<remote exception truncated: {reason}>")

    def _reference(self, node_id):
        return RemoteExceptionReference(f"<remote exception reference: node {node_id}>")

    def _bounded_text(self, value):
        return _truncate_text(value, self.text_budget) if type(value) is str else ""

    def _resolve_optional_edge(self, edge):
        if not edge:
            return None
        resolved = self._resolve_edges((edge,), set())
        return resolved[0] if resolved else self._truncated("malformed-reference")

    def _instantiate_non_groups(self):
        for node_id, node in self.raw_nodes.items():
            if self._native_group_class(node) is not None:
                continue
            module, name = self._identity(node)
            cls = _resolve_exception_class(
                module, name, self.import_custom_exceptions,
                self.instantiate_custom_exceptions,
            )
            if cls is None:
                cls = _generic_exception_class(module, name)
            exc = _instantiate(cls)
            self._set_args(exc, self._bounded_data(_safe_tuple(node[3])))
            self._add_notes(exc, self._bounded_notes(node[5]))
            self._set_attrs(exc, self._bounded_data(node[4]))
            self.objects[node_id] = exc

    def _materialize_group(self, node_id, active, depth=0):
        cached = self.objects.get(node_id)
        if cached is not None:
            return cached

        node = self.raw_nodes[node_id]
        group_cls = self._native_group_class(node)
        if group_cls is None or node_id in active or depth >= MAX_EXCEPTION_DEPTH:
            return self._reference(node_id)

        active.add(node_id)
        try:
            children = self._resolve_edges(node[6], active, depth + 1, hierarchy=True)
            try:
                if not children:
                    children = (self._truncated("empty-exception-group"),)
                message = self._bounded_text(node[2])
                try:
                    exc = _get_exception_class(group_cls)(message, tuple(children))
                except Exception:
                    try:
                        exc = group_cls(message, tuple(children))
                    except Exception:
                        if children:
                            children = (self._truncated("hierarchy-cycle"),)
                        exc = _generic_exception_class(*self._identity(node))(message)
                        exc._remote_exceptions = tuple(children)

                self._add_notes(exc, self._bounded_notes(node[5]))
                self._set_attrs(exc, self._bounded_data(node[4]))
                self.objects[node_id] = exc
                return exc
            except Exception:
                return self._reference(node_id)
        finally:
            active.discard(node_id)

    def _resolve_edges(self, edges, active, depth=0, hierarchy=False):
        if type(edges) is not tuple:
            return ()
        result = []
        for edge in edges[:MAX_EXCEPTION_CHILDREN]:
            if type(edge) is not tuple or len(edge) != 2 or type(edge[0]) is not str:
                result.append(self._truncated("malformed-exception"))
                continue
            label, target = edge
            if label == TRUNCATED:
                result.append(self._truncated(target))
            elif label in (NODE, REFERENCE):
                if type(target) is not int:
                    result.append(self._truncated("malformed-reference"))
                elif target >= MAX_EXCEPTION_NODES:
                    result.append(self._truncated("node-limit"))
                elif target not in self.raw_nodes:
                    result.append(self._truncated("malformed-reference"))
                elif target in active:
                    result.append(self._reference(target))
                elif depth >= MAX_EXCEPTION_DEPTH:
                    result.append(self._truncated("depth-limit"))
                elif self._native_group_class(self.raw_nodes[target]) is not None:
                    result.append(self._materialize_group(target, active, depth))
                else:
                    result.append(self.objects.get(target, self._truncated("malformed-reference")))
            else:
                result.append(self._truncated("malformed-exception"))
        if hierarchy and len(edges) > MAX_EXCEPTION_CHILDREN:
            result.append(self._truncated("children-limit"))
        return tuple(result)

    def _materialize_groups(self):
        for node_id, node in self.raw_nodes.items():
            if self._native_group_class(node) is not None:
                self._materialize_group(node_id, self.active_groups)

    def _link(self):
        for node_id, node in self.raw_nodes.items():
            exc = self.objects.get(node_id)
            if exc is None:
                continue
            children = self._resolve_edges(node[6], set(), hierarchy=True)
            module, name = self._identity(node)
            reason_markers = tuple(
                (index, edge[1])
                for index, edge in enumerate(_safe_tuple(node[6])[:MAX_EXCEPTION_CHILDREN])
                if type(edge) is tuple and len(edge) == 2 and edge[0] == TRUNCATED
            )
            try:
                exc._remote_node_id = node_id
                exc._remote_type_name = f"{module}.{name}"
                exc._remote_message = self._bounded_text(node[2])
                exc._remote_children = children
                exc._remote_truncated_children = reason_markers
                exc._remote_tb = self._bounded_text(node[10])
            except Exception:
                pass

            if self._native_group_class(node) is None and children:
                try:
                    exc._remote_exceptions = children
                except Exception:
                    pass

            cause_edge = node[7]
            context_edge = node[8]
            cause = self._resolve_optional_edge(cause_edge)
            context = self._resolve_optional_edge(context_edge)
            for attr, value in (("__cause__", cause), ("__context__", context)):
                if value is not None:
                    try:
                        setattr(exc, attr, value)
                    except Exception:
                        pass
            try:
                exc.__suppress_context__ = bool(node[9])
            except Exception:
                pass

    def load(self):
        self._instantiate_non_groups()
        self._materialize_groups()
        self._link()
        root = self.objects.get(self.root_id)
        if root is None:
            root = self._truncated("missing-or-malformed-root")
        try:
            root._remote_exception_tree = root
            root._remote_exception_root_id = self.root_id
            root._remote_exception_nodes = tuple(self.objects.get(node_id)
                                                 for node_id in sorted(self.objects))
            root._remote_exception_raw_nodes = tuple(self.raw_nodes[node_id]
                                                     for node_id in sorted(self.raw_nodes))
        except Exception:
            pass
        return root


def load_structured(val, import_custom_exceptions, instantiate_custom_exceptions,
                    instantiate_oldstyle_exceptions):
    try:
        if not _is_structured(val):
            raise ValueError("malformed structured exception")
        return _StructuredLoader(
            val, import_custom_exceptions, instantiate_custom_exceptions,
            instantiate_oldstyle_exceptions,
        ).load()
    except Exception as error:
        fallback = GenericException(
            f"<malformed structured remote exception: {type(error).__name__}: {error}>"
        )
        fallback._remote_tb = "<malformed structured remote exception>"
        fallback._remote_exception_tree = fallback
        fallback._remote_exception_nodes = (fallback,)
        return fallback


def load_legacy(val, import_custom_exceptions, instantiate_custom_exceptions,
                instantiate_oldstyle_exceptions):
    if val == consts.EXC_STOP_ITERATION:
        return StopIteration
    if type(val) is str:
        return val

    (modname, clsname), args, attrs, tbtext = val
    cls = _resolve_exception_class(
        modname, clsname, import_custom_exceptions, instantiate_custom_exceptions,
    )

    if cls is None:
        cls = _generic_exception_class(modname, clsname)
    elif not instantiate_oldstyle_exceptions:
        if not isinstance(cls, type) or not issubclass(cls, BaseException):
            cls = _generic_exception_class(modname, clsname)

    cls = _get_exception_class(cls)
    if ClassType is not type and isinstance(cls, ClassType):
        exc = InstanceType(cls)
    else:
        exc = cls.__new__(cls)

    exc.args = args
    for name, attrval in attrs:
        try:
            setattr(exc, name, attrval)
        except AttributeError:
            pass

    remote_ver = getattr(exc, "_remote_version", "<version denied>")
    if remote_ver != "<version denied>" and remote_ver.split(".")[0] != str(version.version[0]):
        warning = "\nWARNING: Remote is on RPyC {} and local is on RPyC {}.\n\n"
        tbtext += warning.format(remote_ver, version.__version__)

    exc._remote_tb = tbtext
    return exc


def load(val, import_custom_exceptions, instantiate_custom_exceptions,
         instantiate_oldstyle_exceptions, structured=False):
    if structured and _is_structured(val):
        return load_structured(
            val, import_custom_exceptions, instantiate_custom_exceptions,
            instantiate_oldstyle_exceptions,
        )
    return load_legacy(
        val, import_custom_exceptions, instantiate_custom_exceptions,
        instantiate_oldstyle_exceptions,
    )
