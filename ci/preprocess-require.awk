# Copyright 2025, 2026 Query Farm LLC - https://query.farm
#
# Rewrite each `require <ext>` gate in an upstream vgi sqllogictest into an
# explicit signed INSTALL+LOAD, so the prebuilt standalone `haybarn-unittest`
# (which links none of these extensions) can run the suite. The vgi extension
# comes from the signed community channel; httpfs/json/parquet/spatial from the
# signed core channel. `require-env` and every other directive pass through
# untouched. See ci/README.md.
#
# With `-v http=1`, also inject a signed `INSTALL httpfs FROM core; LOAD httpfs;`
# before the first worker ATTACH (keyed off `require vgi` or `require-env
# VGI_TEST_WORKER`). The prebuilt `haybarn-unittest` does not statically link
# httpfs, so `ATTACH ... (TYPE vgi, LOCATION 'http://...')` fails with a binder
# error unless httpfs is loaded into the connection first. Upstream's own
# `unittest` build links httpfs, so its `make test_http` needs no such step.
#
# With `-v install=0` the `require` rewrites are suppressed and each gate passes
# through untouched — for a *from-source* `unittest`, which is built against a
# development DuckDB/haybarn version with no published community `vgi` to install
# and resolves `require vgi` from the statically linked extension instead.
#
# The httpfs injection happens either way, and it is not optional: 24 files in the
# suite ATTACH an `http://` worker without a `require httpfs` line of their own,
# and the extension rejects that with "VGI HTTP transport requires the httpfs
# extension" — a Binder Error whose text contains "HTTP", which sqllogictest's
# default `ignore_error_messages` turns into yet another silent skip.
#
# `install` defaults to 1, so with the flag unset this file's output is
# the same as the standalone release-runner mode.
BEGIN { injected = 0; if (install == "") install = 1 }
function inject_httpfs() {
    if (http != 1 || injected) return
    print "";
    if (install) {
        print "statement ok"; print "INSTALL httpfs FROM core;"; print "";
        print "statement ok"; print "LOAD httpfs;";
    } else {
        # A from-source build already has httpfs in its own local repository, so an
        # explicit `INSTALL ... FROM core` is rejected for a differing origin. Let
        # DuckDB's own autoload do it — the same path the 230 files that *do* carry
        # a `require httpfs` line already take on that binary.
        print "require httpfs";
    }
    injected = 1
}
/^require[ \t]+vgi[ \t]*$/ {
    if (install) {
        print "statement ok"; print "INSTALL vgi FROM community;"; print "";
        print "statement ok"; print "LOAD vgi;";
    } else print
    inject_httpfs();
    next
}
/^require[ \t]+(httpfs|json|parquet|spatial)[ \t]*$/ {
    if (!install) { print; next }
    ext = $2
    print "statement ok"; print "INSTALL " ext " FROM core;"; print "";
    print "statement ok"; print "LOAD " ext ";"; next
}
/^require-env[ \t]+VGI_TEST_WORKER[ \t]*$/ {
    print
    inject_httpfs();
    next
}
{ print }
