#!/usr/bin/perl

# Tests the import of 'raw+size' streams with real dd processes: a stream that ends before the size
# from its header must not count as imported.

use v5.36;

use lib qw(..);

use File::Temp qw(tempdir);
use Test::More;

use PVE::Storage;
use PVE::Storage::DirPlugin;
use PVE::Storage::Plugin;

my $tmpdir = tempdir('import-stream-XXXXXX', TMPDIR => 1, CLEANUP => 1);

# Returns a file handle that delivers $data.
my sub stream_of($data) {
    my $path = "$tmpdir/stream";
    open(my $out, '>', $path) or die "unable to write '$path' - $!\n";
    print $out $data;
    close($out);
    open(my $fh, '<', $path) or die "unable to read '$path' - $!\n";
    return $fh;
}

my sub content_of($path) {
    open(my $fh, '<', $path) or return undef;
    local $/ = undef;
    return <$fh>;
}

# Runs $code and returns its error, with the output of the commands it runs discarded.
my sub error_of($code) {
    open(my $saved_out, '>&', \*STDOUT) or die "unable to duplicate STDOUT - $!\n";
    open(my $saved_err, '>&', \*STDERR) or die "unable to duplicate STDERR - $!\n";
    open(STDOUT, '>', '/dev/null') or die "unable to redirect STDOUT - $!\n";
    open(STDERR, '>', '/dev/null') or die "unable to redirect STDERR - $!\n";
    eval { $code->() };
    my $err = $@;
    open(STDOUT, '>&', $saved_out) or die "unable to restore STDOUT - $!\n";
    open(STDERR, '>&', $saved_err) or die "unable to restore STDERR - $!\n";
    return $err;
}

subtest 'raw stream written with dd' => sub {
    my $data = join('', map { chr($_ % 251) } 1 .. 70_000);
    my $target = "$tmpdir/target";
    my $import = sub($sent, $size, $opts = undef) {
        unlink($target);
        my $fh = stream_of($sent);
        return error_of(sub {
            PVE::Storage::Plugin::import_raw_stream($fh, $target, $size, $opts);
        });
    };

    is($import->($data, length($data)), '', 'complete stream is accepted');
    is(content_of($target), $data, 'all of its data got written');

    my $half = substr($data, 0, 35_000);
    is(
        $import->($half, length($data)),
        "import: stream ended after 35000 of 70000 bytes\n",
        'stream that ends early is refused',
    );
    is(
        $import->(substr($data, 0, 69_999), length($data)),
        "import: stream ended after 69999 of 70000 bytes\n",
        'stream that lacks its last byte is refused',
    );
    is(
        $import->($data, 69_999),
        "import: stream has 70000 bytes, but its header announced 69999\n",
        'stream with more data than announced is refused',
    );
    is($import->('', length($data)), "import: stream ended after 0 of 70000 bytes\n",
        'no data');
    # dd reports a single byte in the singular
    is(
        $import->('x', length($data)),
        "import: stream ended after 1 of 70000 bytes\n",
        'stream of one byte is refused',
    );
    is($import->('x', 1), '', 'complete stream of one byte is accepted');
    is($import->($half, undef), '', 'no check without a size');

    is($import->($data, length($data), ['conv=sparse']), '', 'options are passed to dd');
    is(content_of($target), $data, 'data got written with options');
    open(my $existing, '>', "$tmpdir/existing") or die "unable to create file - $!\n";
    close($existing);
    my $fh = stream_of($data);
    my $err = error_of(sub {
        PVE::Storage::Plugin::import_raw_stream(
            $fh, "$tmpdir/existing", length($data), ['conv=excl'],
        );
    });
    like($err, qr/^command 'dd .*conv=excl.* failed/, 'failure of dd is reported');
};

subtest 'import into a directory storage' => sub {
    my $path = "$tmpdir/store";
    mkdir($path) or die "unable to create '$path' - $!\n";
    my $scfg = { path => $path, type => 'dir', content => { images => 1 } };
    my $volname = '100/vm-100-disk-0.raw';
    my $file = "$path/images/$volname";
    my $size = 1024 * 1024;
    my $data = join('', map { chr($_ % 256) } 1 .. $size);

    my $import = sub($sent) {
        my $fh = stream_of(pack('Q<', $size) . $sent);
        my $volid;
        my $err = error_of(sub {
            $volid = PVE::Storage::DirPlugin->volume_import(
                $scfg, 'store', $fh, $volname, 'raw+size', undef, undef, 0, 0,
            );
        });
        return ($volid, $err);
    };

    my ($volid, $err) = $import->(substr($data, 0, $size / 2));
    is(
        $err,
        "import: stream ended after 524288 of 1048576 bytes\n",
        'cut-off stream is refused',
    );
    ok(!-e $file, 'its volume got removed again');

    ($volid, $err) = $import->($data);
    is($err, '', 'complete stream is imported');
    is($volid, "store:$volname", 'volume ID is returned');
    is(content_of($file), $data, 'volume has the data of the stream');

    # qemu-img rounds the size of a raw file up to full sectors, the stream has the real length
    my $odd = substr($data, 0, 1_000_003);
    my $snippet = 'snippets/odd.yaml';
    mkdir("$path/snippets") or die "unable to create the snippets directory - $!\n";
    open(my $out, '>', "$path/snippets/odd.yaml") or die "unable to write the snippet - $!\n";
    print $out $odd;
    close($out);

    my $stream = "$tmpdir/exported";
    open(my $export_fh, '>', $stream) or die "unable to write '$stream' - $!\n";
    $err = error_of(sub {
        PVE::Storage::DirPlugin->volume_export(
            $scfg, 'store', $export_fh, $snippet, 'raw+size', undef, undef, 0,
        );
    });
    close($export_fh);
    is($err, '', 'file of odd length is exported');
    my $exported = content_of($stream);
    is(unpack('Q<', substr($exported, 0, 8)), 1_000_003, 'header has the length of the file');
    is(substr($exported, 8), $odd, 'stream has the data of the file');

    unlink("$path/snippets/odd.yaml");
    open(my $import_fh, '<', $stream) or die "unable to read '$stream' - $!\n";
    $err = error_of(sub {
        PVE::Storage::DirPlugin->volume_import(
            $scfg, 'store', $import_fh, $snippet, 'raw+size', undef, undef, 0, 0,
        );
    });
    is($err, '', 'its stream is imported');
    is(content_of("$path/snippets/odd.yaml"), $odd, 'imported file has the same data');
};

done_testing();
