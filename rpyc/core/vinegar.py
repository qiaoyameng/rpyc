"""
**Vinegar** ("when things go sour") is a safe serializer for exceptions.
The :data:`configuration parameters <rpyc.core.protocol.DEFAULT_CONFIG>` control
its mode of operation, for instance, whether to allow *old-style* exceptions
(that do not derive from ``Exception``), whether to allow the :func:`load` to
import custom modules (imposes a security risk), etc.

Note that by changing the configuration parameters, this module can be made
non-secure. Keep this in mind.
"""
import sys
import traceback
try:
    import exceptions as exceptions_module
except ImportError:
    import builtins as exceptions_module
try:
    from types import InstanceType, ClassType
except ImportError:
    ClassType = type

from rpyc.core import brine
from rpyc.core import consts
from rpyc import version


REMOTE_LINE_START = "\n\n========= Remote Traceback "
REMOTE_LINE_END = " =========\n"
REMOTE_LINE = "{0}({{}}){1}".format(REMOTE_LINE_START, REMOTE_LINE_END)

# ===============================================================================
# Exception trees (wire capability "exc_tree_v1")
#
# The legacy format produced by :func:`dump` flattens an exception into a single
# tuple of ``(type, args, attrs, traceback text)``. It cannot represent
# ``ExceptionGroup`` hierarchies, ``__notes__`` or ``__cause__``/``__context__``
# chains. The tree format below encodes every involved exception as a node in a
# flat, brine-safe node list; edges (group children, cause, context) are integer
# references into that list, so shared objects and reference cycles survive the
# round trip without unbounded recursion.
#
# Only safe, brine-dumpable values (tuples, strings, ints, bools, None) are put
# on the wire. Decoding never calls a remote-specified constructor: instances
# are created with ``cls.__new__(cls)`` under the same import/instantiation
# policy as the legacy loader, and unknown types degrade to a
# :class:`GenericException` subclass named after the remote type. The only
# exception are the *built-in* ``ExceptionGroup``/``BaseExceptionGroup`` classes,
# which are reconstructed from their already-decoded (safe) children.

#: capability token exchanged during connection capability negotiation
EXC_TREE_CAPABILITY = "exc_tree_v1"
#: tag identifying the tree format on the wire (first element of the dumped tuple)
EXC_TREE_TAG = "rpyc.exc_tree.v1"

_KIND_EXCEPTION = "exc"
_KIND_GROUP = "group"
_KIND_TRUNCATED = "truncated"

#: edge value used for "no cause/context"
NO_REF = -1

#: truncation reasons used in placeholder nodes and text markers
TRUNCATION_MARK_DEPTH = "depth"
TRUNCATION_MARK_NODES = "nodes"
TRUNCATION_MARK_TEXT = "text"

#: default safety limits (overridable through the connection config)
DEFAULT_MAX_NODES = 64
DEFAULT_MAX_DEPTH = 16
DEFAULT_MAX_TEXT = 8192

#: number of fields in a serialized node tuple
_NODE_LEN = 13

try:
    _BASE_EXCEPTION_GROUP = BaseExceptionGroup
    _BUILTIN_EXCEPTION_GROUPS = (ExceptionGroup, BaseExceptionGroup)
except NameError:  # Python < 3.11 has no exception groups
    _BASE_EXCEPTION_GROUP = None
    _BUILTIN_EXCEPTION_GROUPS = ()

# attributes that must not be copied into the tree (handled explicitly or unsafe)
_TREE_IGNORED_ATTRS = frozenset(["_remote_tb", "with_traceback", "exceptions"])


def dump(typ, val, tb, include_local_traceback, include_local_version):
    """Dumps the given exceptions info, as returned by ``sys.exc_info()``

    :param typ: the exception's type (class)
    :param val: the exceptions' value (instance)
    :param tb: the exception's traceback (a ``traceback`` object)
    :param include_local_traceback: whether or not to include the local traceback
                                    in the dumped info. This may expose the other
                                    side to implementation details (code) and
                                    package structure, and may theoretically impose
                                    a security risk.

    :returns: A tuple of ``((module name, exception name), arguments, attributes,
              traceback text)``. This tuple can be safely passed to
              :func:`brine.dump <rpyc.core.brine.dump>`
    """
    if typ is StopIteration:
        return consts.EXC_STOP_ITERATION  # optimization
    if type(typ) is str:
        return typ

    if include_local_traceback:
        tbtext = "".join(traceback.format_exception(typ, val, tb))
    else:
        tbtext = "<traceback denied>"
    attrs = []
    args = []
    ignored_attrs = frozenset(["_remote_tb", "with_traceback"])
    for name in dir(val):
        if name == "args":
            for a in val.args:
                if brine.dumpable(a):
                    args.append(a)
                else:
                    args.append(repr(a))
        elif name.startswith("_") or name in ignored_attrs:
            continue
        else:
            try:
                attrval = getattr(val, name)
            except AttributeError:
                # skip this attr. see issue #108
                continue
            if not brine.dumpable(attrval):
                attrval = repr(attrval)
            attrs.append((name, attrval))
    if include_local_version:
        attrs.append(("_remote_version", version.__version__))
    else:
        attrs.append(("_remote_version", "<version denied>"))
    return (typ.__module__, typ.__name__), tuple(args), tuple(attrs), tbtext


def load(val, import_custom_exceptions, instantiate_custom_exceptions, instantiate_oldstyle_exceptions,
         max_nodes=DEFAULT_MAX_NODES):
    """
    Loads a dumped exception (the tuple returned by :func:`dump`) info a
    throwable exception object. If the exception cannot be instantiated for any
    reason (i.e., the security parameters do not allow it, or the exception
    class simply doesn't exist on the local machine), a :class:`GenericException`
    instance will be returned instead, containing all of the original exception's
    details.

    :param val: the dumped exception
    :param import_custom_exceptions: whether to allow this function to import custom modules
                                     (imposes a security risk)
    :param instantiate_custom_exceptions: whether to allow this function to instantiate "custom
                                          exceptions" (i.e., not one of the built-in exceptions,
                                          such as ``ValueError``, ``OSError``, etc.)
    :param instantiate_oldstyle_exceptions: whether to allow this function to instantiate exception
                                            classes that do not derive from ``BaseException``.
                                            This is required to support old-style exceptions.
                                            Not applicable for Python 3 and above.
    :param max_nodes: safety limit on the number of nodes accepted when ``val``
                      is an exception tree (see :func:`dump_tree`)

    :returns: A throwable exception object
    """
    if val == consts.EXC_STOP_ITERATION:
        return StopIteration  # optimization
    if type(val) is str:
        return val  # deprecated string exceptions
    if isinstance(val, tuple) and len(val) == 3 and val[0] == EXC_TREE_TAG:
        try:
            return _load_tree(val, import_custom_exceptions, instantiate_custom_exceptions, max_nodes)
        except Exception as exc:
            # a malformed/hostile payload must never break the connection's
            # decoding pipeline -- degrade to a generic exception instead
            return _decode_error_exception(exc)

    (modname, clsname), args, attrs, tbtext = val

    cls = _resolve_exception_class(modname, clsname, import_custom_exceptions, instantiate_custom_exceptions)
    if cls is None:
        cls = _generic_exception_class(modname, clsname)

    cls = _get_exception_class(cls)

    # support old-style exception classes
    if ClassType is not type and isinstance(cls, ClassType):
        exc = InstanceType(cls)
    else:
        exc = cls.__new__(cls)

    exc.args = args
    for name, attrval in attrs:
        try:
            setattr(exc, name, attrval)
        except AttributeError:      # handle immutable attrs (@property)
            pass

    # When possible and relevant, warn the user about mismatch in major versions between remote and local
    remote_ver = getattr(exc, "_remote_version", "<version denied>")
    if remote_ver != "<version denied>" and remote_ver.split('.')[0] != str(version.version[0]):
        _warn = '\nWARNING: Remote is on RPyC {} and local is on RPyC {}.\n\n'
        tbtext += _warn.format(remote_ver, version.__version__)

    exc._remote_tb = tbtext
    return exc


class GenericException(Exception):
    """A 'generic exception' that is raised when the exception the gotten from
    the other party cannot be instantiated locally"""
    pass


_generic_exceptions_cache = {}
_exception_classes_cache = {}


def _resolve_exception_class(modname, clsname, import_custom_exceptions, instantiate_custom_exceptions):
    """Resolves ``(modname, clsname)`` to an exception class, honoring the
    import/instantiation security policy. Returns ``None`` when the class may
    not (or cannot) be used locally."""
    if import_custom_exceptions and modname not in sys.modules:
        try:
            __import__(modname, None, None, "*")
        except Exception:
            pass

    if instantiate_custom_exceptions:
        if modname in sys.modules:
            cls = getattr(sys.modules[modname], clsname, None)
        else:
            cls = None
    elif modname == exceptions_module.__name__:
        cls = getattr(exceptions_module, clsname, None)
    else:
        cls = None

    if not isinstance(cls, type) or not issubclass(cls, BaseException):
        cls = None
    return cls


def _generic_exception_class(modname, clsname):
    """Returns a cached :class:`GenericException` subclass carrying the remote
    type's name, used when the real class may not be instantiated locally."""
    fullname = f"{modname}.{clsname}"
    # py2: `type()` expects `str` not `unicode`!
    fullname = str(fullname)
    if fullname not in _generic_exceptions_cache:
        fakemodule = {"__module__": f"{__name__}/{modname}"}
        if isinstance(GenericException, ClassType):
            _generic_exceptions_cache[fullname] = ClassType(fullname, (GenericException,), fakemodule)
        else:
            _generic_exceptions_cache[fullname] = type(fullname, (GenericException,), fakemodule)
    return _generic_exceptions_cache[fullname]


def _get_exception_class(cls):
    if cls in _exception_classes_cache:
        return _exception_classes_cache[cls]

    # subclass the exception class' to provide a version of __str__ that supports _remote_tb
    class Derived(cls):
        def __str__(self):
            try:
                text = cls.__str__(self)
            except Exception:
                text = "<Unprintable exception>"
            if hasattr(self, "_remote_tb"):
                text += REMOTE_LINE.format(self._remote_tb.count(REMOTE_LINE_START) + 1)
                text += self._remote_tb
            return text

        def __repr__(self):
            return str(self)

    Derived.__name__ = cls.__name__
    Derived.__module__ = cls.__module__
    _exception_classes_cache[cls] = Derived
    return Derived


# ===============================================================================
# exception trees
# ===============================================================================

def _truncate_text(text, max_text):
    if max_text is not None and max_text >= 0 and len(text) > max_text:
        return text[:max_text] + "\n... <truncated>"
    return text


def _safe_repr(value):
    try:
        return repr(value)
    except Exception:
        return "<unrepresentable object>"


def _safe_tree_value(value, max_text):
    """Coerces ``value`` into a brine-dumpable, size-limited value."""
    if not brine.dumpable(value):
        value = _safe_repr(value)
    if isinstance(value, str):
        value = _truncate_text(value, max_text)
    return value


def dump_tree(typ, val, tb, include_local_traceback, include_local_version,
              max_nodes=DEFAULT_MAX_NODES, max_depth=DEFAULT_MAX_DEPTH,
              max_text=DEFAULT_MAX_TEXT):
    """Dumps the given exception (as returned by ``sys.exc_info()``) into an
    exception *tree*: a flat list of nodes that preserves ``ExceptionGroup``
    hierarchies, ``__notes__`` and ``__cause__``/``__context__`` relations.

    Edges between nodes are integer references into the node list, so shared
    objects and reference cycles (e.g. a manual cause cycle) are encoded as
    references instead of being recursed into. When ``max_nodes`` or
    ``max_depth`` is exceeded, an explicit *truncation marker* node is emitted
    in place of the subtree; overly long texts are cut and suffixed with a
    truncation marker.

    The result only contains brine-safe values and can be passed to
    :func:`brine.dump <rpyc.core.brine.dump>` directly.

    :returns: A tuple of ``(EXC_TREE_TAG, nodes, root_id)``
    """
    nodes = []
    seen = {}       # id(exception) -> node id; makes shared refs and cycles explicit
    markers = {}    # truncation reason -> shared placeholder node id

    def marker_node_id(reason, exc):
        node_id = markers.get(reason)
        if node_id is None:
            node_id = len(nodes)
            nodes.append((
                node_id, _KIND_TRUNCATED, str(type(exc).__module__), str(type(exc).__name__),
                (f"<remote exception truncated: {reason}>",), (), (), "",
                NO_REF, NO_REF, False, (), reason))
            markers[reason] = node_id
        return node_id

    def dump_node(exc, exc_tb, depth):
        obj_id = id(exc)
        if obj_id in seen:
            return seen[obj_id]  # shared reference / cycle: refer to the existing node
        if len(nodes) >= max_nodes:
            return marker_node_id(TRUNCATION_MARK_NODES, exc)
        if depth >= max_depth:
            return marker_node_id(TRUNCATION_MARK_DEPTH, exc)
        node_id = len(nodes)
        seen[obj_id] = node_id
        nodes.append(None)  # reserve the slot up front to break cause/context cycles

        exc_type = type(exc)
        is_group = _BASE_EXCEPTION_GROUP is not None and isinstance(exc, _BASE_EXCEPTION_GROUP)
        kind = _KIND_GROUP if is_group else _KIND_EXCEPTION

        args = tuple(_safe_tree_value(a, max_text) for a in getattr(exc, "args", ()))
        if is_group:
            # BaseExceptionGroup.args is (message, exceptions); the children
            # are already encoded as edges, so only keep the message
            args = args[:1]

        attrs = []
        for name in dir(exc):
            if name == "args" or name.startswith("_") or name in _TREE_IGNORED_ATTRS:
                continue
            try:
                attrval = getattr(exc, name)
            except Exception:
                # skip this attr. see issue #108
                continue
            if callable(attrval):
                # methods (e.g. ``add_note``) are behavior, not data
                continue
            attrs.append((name, _safe_tree_value(attrval, max_text)))
        if depth == 0:
            if include_local_version:
                attrs.append(("_remote_version", version.__version__))
            else:
                attrs.append(("_remote_version", "<version denied>"))

        notes = []
        for note in getattr(exc, "__notes__", None) or ():
            if not isinstance(note, str):
                note = _safe_repr(note)
            notes.append(_truncate_text(note, max_text))

        if include_local_traceback:
            node_tb = exc_tb if exc_tb is not None else getattr(exc, "__traceback__", None)
            tbtext = "".join(traceback.format_tb(node_tb)) if node_tb is not None else ""
        else:
            tbtext = "<traceback denied>"
        tbtext = _truncate_text(tbtext, max_text)

        cause = getattr(exc, "__cause__", None)
        if isinstance(cause, BaseException):
            cause_id = dump_node(cause, None, depth + 1)
        else:
            cause_id = NO_REF
        context = getattr(exc, "__context__", None)
        if isinstance(context, BaseException):
            context_id = dump_node(context, None, depth + 1)
        else:
            context_id = NO_REF
        suppress_context = bool(getattr(exc, "__suppress_context__", False))

        children = ()
        if is_group:
            try:
                child_excs = tuple(exc.exceptions)
            except Exception:
                child_excs = ()
            children = tuple(dump_node(child, None, depth + 1) for child in child_excs)

        nodes[node_id] = (node_id, kind, str(exc_type.__module__), str(exc_type.__name__),
                          args, tuple(attrs), tuple(notes), tbtext,
                          cause_id, context_id, suppress_context, children, "")
        return node_id

    root_id = dump_node(val, tb, 0)
    return (EXC_TREE_TAG, tuple(nodes), root_id)


def _decode_error_exception(error):
    """Builds the fallback exception used when a tree payload cannot be decoded."""
    cls = _get_exception_class(_generic_exception_class(__name__, "UndecodableRemoteException"))
    exc = cls.__new__(cls)
    exc.args = (f"failed to decode remote exception tree: {error!r}",)
    exc._remote_tb = ""
    return exc


def _load_tree(val, import_custom_exceptions, instantiate_custom_exceptions, max_nodes=DEFAULT_MAX_NODES):
    """Loads a dumped exception tree (see :func:`dump_tree`) into a throwable
    exception object. Raises on malformed payloads; callers are expected to
    degrade gracefully."""
    _tag, nodes, root_id = val
    if isinstance(root_id, bool) or not isinstance(root_id, int):
        raise ValueError("malformed exception tree: invalid root reference")
    if not isinstance(nodes, tuple):
        raise ValueError("malformed exception tree: nodes must be a tuple")
    # hard limit against hostile payloads (allows the small marker-node slack
    # that dump_tree may add on top of max_nodes)
    hard_limit = min(max(int(max_nodes), 1) * 4 + 8, 1024)
    if len(nodes) > hard_limit:
        raise ValueError(f"malformed exception tree: too many nodes ({len(nodes)})")
    nodes_by_id = {}
    for node in nodes:
        if not isinstance(node, tuple) or len(node) != _NODE_LEN:
            raise ValueError("malformed exception tree: invalid node")
        nodes_by_id[node[0]] = node
    if root_id not in nodes_by_id:
        raise ValueError("malformed exception tree: unknown root reference")

    _MISSING = object()
    _IN_PROGRESS = object()
    memo = {}

    def decode_marker(message):
        cls = _get_exception_class(_generic_exception_class(__name__, "TruncatedRemoteException"))
        exc = cls.__new__(cls)
        exc.args = (message,)
        exc._remote_tb = ""
        return exc

    def valid_ref(ref):
        return isinstance(ref, int) and not isinstance(ref, bool) and ref in nodes_by_id

    def build(node_id):
        inst = memo.get(node_id, _MISSING)
        if inst is _IN_PROGRESS:
            # a group may not (transitively) contain itself
            return decode_marker(f"<cycle through exception group node {node_id}>")
        if inst is not _MISSING:
            return inst
        node = nodes_by_id.get(node_id)
        if node is None:
            return decode_marker(f"<invalid exception node reference {node_id}>")
        (_id, kind, modname, clsname, args, attrs, notes, tbtext,
         cause_id, context_id, suppress_context, children, _truncated) = node
        memo[node_id] = _IN_PROGRESS

        if kind == _KIND_TRUNCATED:
            # truncation placeholders always decode to a generic marker so they
            # can never be mistaken for a successfully transferred exception
            cls = None
        else:
            cls = _resolve_exception_class(str(modname), str(clsname),
                                           import_custom_exceptions, instantiate_custom_exceptions)
        is_group = kind == _KIND_GROUP

        exc = None
        if is_group and cls in _BUILTIN_EXCEPTION_GROUPS:
            # the built-in groups are safe to reconstruct from decoded children;
            # custom constructors are never called (see module docs)
            child_objs = tuple(build(c) for c in children if valid_ref(c))
            message = args[0] if args else ""
            if not isinstance(message, str):
                message = str(message)
            if cls is ExceptionGroup and not all(isinstance(c, Exception) for c in child_objs):
                cls = BaseExceptionGroup
            try:
                exc = _get_exception_class(cls)(message, child_objs)
            except Exception:
                exc = None

        if exc is None:
            if cls is None:
                cls = _generic_exception_class(str(modname) or __name__, str(clsname) or "RemoteException")
            cls = _get_exception_class(cls)
            try:
                exc = cls.__new__(cls)
            except Exception:
                cls = _get_exception_class(_generic_exception_class(str(modname) or __name__,
                                                                    str(clsname) or "RemoteException"))
                exc = cls.__new__(cls)
            # memoize *before* linking so that cause/context cycles and shared
            # references resolve to this very instance
            memo[node_id] = exc
            try:
                exc.args = tuple(args)
            except Exception:
                pass
            if is_group:
                child_objs = tuple(build(c) for c in children if valid_ref(c))
                for attr in ("_exceptions", "exceptions"):
                    try:
                        setattr(exc, attr, child_objs)
                    except Exception:
                        pass
        else:
            memo[node_id] = exc
            try:
                exc.args = tuple(args)
            except Exception:
                pass

        remote_version = None
        for item in attrs:
            if not (isinstance(item, tuple) and len(item) == 2):
                continue
            name, attrval = item
            if name == "_remote_version":
                remote_version = attrval
            try:
                setattr(exc, name, attrval)
            except Exception:
                pass

        if notes:
            try:
                exc.__notes__ = [n if isinstance(n, str) else str(n) for n in notes]
            except Exception:
                pass

        try:
            exc.__suppress_context__ = bool(suppress_context)
        except Exception:
            pass
        if valid_ref(cause_id):
            try:
                exc.__cause__ = build(cause_id)
            except Exception:
                pass
        if valid_ref(context_id):
            try:
                exc.__context__ = build(context_id)
            except Exception:
                pass

        if isinstance(remote_version, str) and remote_version != "<version denied>" \
                and remote_version.split('.')[0] != str(version.version[0]):
            _warn = '\nWARNING: Remote is on RPyC {} and local is on RPyC {}.\n\n'
            tbtext += _warn.format(remote_version, version.__version__)
        try:
            exc._remote_tb = tbtext
        except Exception:
            pass
        return exc

    return build(root_id)
