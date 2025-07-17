package PVE::API2::Storage::Status;

use strict;
use warnings;

use File::Basename;
use File::Path;
use POSIX qw(ENOENT);

use PVE::Cluster;
use PVE::Exception qw(raise_param_exc);
use PVE::INotify;
use PVE::JSONSchema qw(get_standard_option);
use PVE::RESTHandler;
use PVE::RPCEnvironment;
use PVE::RRD;
use PVE::Tools qw(run_command);

use PVE::API2::Storage::Content;
use PVE::API2::Storage::FileRestore;
use PVE::API2::Storage::PruneBackups;
use PVE::Storage;

use base qw(PVE::RESTHandler);

__PACKAGE__->register_method({
    subclass => "PVE::API2::Storage::PruneBackups",
    path => '{storage}/prunebackups',
});

__PACKAGE__->register_method({
    subclass => "PVE::API2::Storage::Content",
    # set fragment delimiter (no subdirs) - we need that, because volume
    # IDs may contain a slash '/'
    fragmentDelimiter => '',
    path => '{storage}/content',
});

__PACKAGE__->register_method({
    subclass => "PVE::API2::Storage::FileRestore",
    path => '{storage}/file-restore',
});

my sub assert_ova_contents {
    my ($file) = @_;

    # test if it's really a tar file with an ovf file inside
    my $hasOvf = 0;
    run_command(
        ['tar', '-t', '-f', $file],
        outfunc => sub {
            my ($line) = @_;

            if ($line =~ m/\.ovf$/) {
                $hasOvf = 1;
            }
        },
    );

    die "ova archive has no .ovf file inside\n" if !$hasOvf;

    return 1;
}

__PACKAGE__->register_method({
    name => 'index',
    path => '',
    method => 'GET',
    description => "Get status for all datastores.",
    permissions => {
        description =>
            "Only list entries where you have 'Datastore.Audit' or 'Datastore.AllocateSpace' permissions on '/storage/<storage>'",
        user => 'all',
    },
    protected => 1,
    proxyto => 'node',
    parameters => {
        additionalProperties => 0,
        properties => {
            node => get_standard_option('pve-node'),
            storage => get_standard_option(
                'pve-storage-id',
                {
                    description => "Only list status for  specified storage",
                    optional => 1,
                    completion => \&PVE::Storage::complete_storage_enabled,
                },
            ),
            content => {
                description => "Only list stores which support this content type.",
                type => 'string',
                format => 'pve-storage-content-list',
                optional => 1,
                completion => \&PVE::Storage::complete_content_type,
            },
            enabled => {
                description => "Only list stores which are enabled (not disabled in config).",
                type => 'boolean',
                optional => 1,
                default => 0,
            },
            target => get_standard_option(
                'pve-node',
                {
                    description =>
                        "If target is different to 'node', we only lists shared storages which "
                        . "content is accessible on this 'node' and the specified 'target' node.",
                    optional => 1,
                    completion => \&PVE::Cluster::get_nodelist,
                },
            ),
            'format' => {
                description => "Include information about formats",
                type => 'boolean',
                optional => 1,
                default => 0,
            },
        },
    },
    returns => {
        type => 'array',
        items => {
            type => "object",
            properties => {
                storage => get_standard_option('pve-storage-id'),
                type => {
                    description => "Storage type.",
                    type => 'string',
                },
                content => {
                    description => "Allowed storage content types.",
                    type => 'string',
                    format => 'pve-storage-content-list',
                },
                enabled => {
                    description => "Set when storage is enabled (not disabled).",
                    type => 'boolean',
                    optional => 1,
                },
                active => {
                    description => "Set when storage is accessible.",
                    type => 'boolean',
                    optional => 1,
                },
                shared => {
                    description => "Shared flag from storage configuration.",
                    type => 'boolean',
                    optional => 1,
                },
                total => {
                    description => "Total storage space in bytes.",
                    type => 'integer',
                    renderer => 'bytes',
                    optional => 1,
                },
                used => {
                    description => "Used storage space in bytes.",
                    type => 'integer',
                    renderer => 'bytes',
                    optional => 1,
                },
                avail => {
                    description => "Available storage space in bytes.",
                    type => 'integer',
                    renderer => 'bytes',
                    optional => 1,
                },
                used_fraction => {
                    description => "Used fraction (used/total).",
                    type => 'number',
                    renderer => 'fraction_as_percentage',
                    optional => 1,
                },
            },
        },
        links => [{ rel => 'child', href => "{storage}" }],
    },
    code => sub {
        my ($param) = @_;

        my $rpcenv = PVE::RPCEnvironment::get();
        my $authuser = $rpcenv->get_user();

        my $localnode = PVE::INotify::nodename();

        my $target = $param->{target};

        undef $target if $target && ($target eq $localnode || $target eq 'localhost');

        my $cfg = PVE::Storage::config();

        my $info = PVE::Storage::storage_info($cfg, $param->{content}, $param->{format});

        raise_param_exc({ storage => "No such storage." })
            if $param->{storage} && !defined($info->{ $param->{storage} });

        my $res = {};
        my @sids = PVE::Storage::storage_ids($cfg);
        foreach my $storeid (@sids) {
            my $data = $info->{$storeid};
            next if !$data;
            my $privs = ['Datastore.Audit', 'Datastore.AllocateSpace'];
            next if !$rpcenv->check_any($authuser, "/storage/$storeid", $privs, 1);
            next if $param->{storage} && $param->{storage} ne $storeid;

            my $scfg = PVE::Storage::storage_config($cfg, $storeid);

            next if $param->{enabled} && $scfg->{disable};

            if ($target) {
                # check if storage content is accessible on local node and specified target node
                # we use this on the Clone GUI

                next if !$scfg->{shared};
                next if !PVE::Storage::storage_check_node($cfg, $storeid, undef, 1);
                next if !PVE::Storage::storage_check_node($cfg, $storeid, $target, 1);
            }

            if ($data->{total}) {
                $data->{used_fraction} = ($data->{used} // 0) / $data->{total};
            }

            # TODO: add support to the storage plugin system to allow returing different supported
            # formats depending on the storage config instead, this is just a stop gap!
            if (lc($data->{type}) eq 'lvm') {
                $data->{format}->[0]->{qcow2} = 0 if !$scfg->{'snapshot-as-volume-chain'};
            }

            $res->{$storeid} = $data;
        }

        return PVE::RESTHandler::hash_to_array($res, 'storage');
    },
});

__PACKAGE__->register_method({
    name => 'diridx',
    path => '{storage}',
    method => 'GET',
    description => "",
    permissions => {
        check => [
            'perm',
            '/storage/{storage}',
            ['Datastore.Audit', 'Datastore.AllocateSpace'],
            any => 1,
        ],
    },
    parameters => {
        additionalProperties => 0,
        properties => {
            node => get_standard_option('pve-node'),
            storage => get_standard_option('pve-storage-id'),
        },
    },
    returns => {
        type => 'array',
        items => {
            type => "object",
            properties => {
                subdir => { type => 'string' },
            },
        },
        links => [{ rel => 'child', href => "{subdir}" }],
    },
    code => sub {
        my ($param) = @_;

        my $res = [
            { subdir => 'content' },
            { subdir => 'download-url' },
            { subdir => 'file-restore' },
            { subdir => 'import-metadata' },
            { subdir => 'prunebackups' },
            { subdir => 'rrd' },
            { subdir => 'rrddata' },
            { subdir => 'status' },
            { subdir => 'upload' },
        ];

        return $res;
    },
});

__PACKAGE__->register_method({
    name => 'read_status',
    path => '{storage}/status',
    method => 'GET',
    description => "Read storage status.",
    permissions => {
        check => [
            'perm',
            '/storage/{storage}',
            ['Datastore.Audit', 'Datastore.AllocateSpace'],
            any => 1,
        ],
    },
    protected => 1,
    proxyto => 'node',
    parameters => {
        additionalProperties => 0,
        properties => {
            node => get_standard_option('pve-node'),
            storage => get_standard_option('pve-storage-id'),
        },
    },
    returns => {
        type => "object",
        properties => {},
    },
    code => sub {
        my ($param) = @_;

        my $cfg = PVE::Storage::config();

        my $info = PVE::Storage::storage_info($cfg, $param->{content});

        my $data = $info->{ $param->{storage} };

        raise_param_exc({ storage => "No such storage." })
            if !defined($data);

        return $data;
    },
});

__PACKAGE__->register_method({
    name => 'rrd',
    path => '{storage}/rrd',
    method => 'GET',
    description => "Read storage RRD statistics (returns PNG).",
    permissions => {
        check => [
            'perm',
            '/storage/{storage}',
            ['Datastore.Audit', 'Datastore.AllocateSpace'],
            any => 1,
        ],
    },
    protected => 1,
    proxyto => 'node',
    parameters => {
        additionalProperties => 0,
        properties => {
            node => get_standard_option('pve-node'),
            storage => get_standard_option('pve-storage-id'),
            timeframe => {
                description => "Specify the time frame you are interested in.",
                type => 'string',
                enum => ['hour', 'day', 'week', 'month', 'year'],
            },
            ds => {
                description => "The list of datasources you want to display.",
                type => 'string',
                format => 'pve-configid-list',
            },
            cf => {
                description => "The RRD consolidation function",
                type => 'string',
                enum => ['AVERAGE', 'MAX'],
                optional => 1,
            },
        },
    },
    returns => {
        type => "object",
        properties => {
            filename => { type => 'string' },
        },
    },
    code => sub {
        my ($param) = @_;

        return PVE::RRD::create_rrd_graph(
            "pve2-storage/$param->{node}/$param->{storage}",
            $param->{timeframe}, $param->{ds}, $param->{cf},
        );
    },
});

__PACKAGE__->register_method({
    name => 'rrddata',
    path => '{storage}/rrddata',
    method => 'GET',
    description => "Read storage RRD statistics.",
    permissions => {
        check => [
            'perm',
            '/storage/{storage}',
            ['Datastore.Audit', 'Datastore.AllocateSpace'],
            any => 1,
        ],
    },
    protected => 1,
    proxyto => 'node',
    parameters => {
        additionalProperties => 0,
        properties => {
            node => get_standard_option('pve-node'),
            storage => get_standard_option('pve-storage-id'),
            timeframe => {
                description => "Specify the time frame you are interested in.",
                type => 'string',
                enum => ['hour', 'day', 'week', 'month', 'year'],
            },
            cf => {
                description => "The RRD consolidation function",
                type => 'string',
                enum => ['AVERAGE', 'MAX'],
                optional => 1,
            },
        },
    },
    returns => {
        type => "array",
        items => {
            type => "object",
            properties => {},
        },
    },
    code => sub {
        my ($param) = @_;

        return PVE::RRD::create_rrd_data(
            "pve2-storage/$param->{node}/$param->{storage}",
            $param->{timeframe},
            $param->{cf},
        );
    },
});

# makes no sense for big images and backup files (because it
# create a copy of the file).
__PACKAGE__->register_method({
    name => 'upload',
    path => '{storage}/upload',
    method => 'POST',
    description => "Upload templates, ISO images, OVAs and VM images.",
    permissions => {
        check => ['perm', '/storage/{storage}', ['Datastore.AllocateTemplate']],
    },
    protected => 1,
    parameters => {
        additionalProperties => 0,
        properties => {
            node => get_standard_option('pve-node'),
            storage => get_standard_option('pve-storage-id'),
            content => {
                description => "Content type.",
                type => 'string',
                format => 'pve-storage-content',
                enum => ['iso', 'vztmpl', 'import'],
            },
            filename => {
                description =>
                    "The name of the file to create. Caution: This will be normalized!",
                maxLength => 255,
                type => 'string',
            },
            checksum => {
                description => "The expected checksum of the file.",
                type => 'string',
                requires => 'checksum-algorithm',
                optional => 1,
            },
            'checksum-algorithm' => {
                description => "The algorithm to calculate the checksum of the file.",
                type => 'string',
                enum => ['md5', 'sha1', 'sha224', 'sha256', 'sha384', 'sha512'],
                requires => 'checksum',
                optional => 1,
            },
            tmpfilename => {
                description =>
                    "The source file name. This parameter is usually set by the REST handler. You can only overwrite it when connecting to the trusted port on localhost.",
                type => 'string',
                optional => 1,
                pattern => '/var/tmp/pveupload-[0-9a-f]+',
            },
        },
    },
    returns => { type => "string" },
    code => sub {
        my ($param) = @_;

        my $rpcenv = PVE::RPCEnvironment::get();

        my $user = $rpcenv->get_user();

        my $cfg = PVE::Storage::config();

        my ($node, $storage) = $param->@{qw(node storage)};
        my $scfg = PVE::Storage::storage_check_enabled($cfg, $storage, $node);

        die "can't upload to storage type '$scfg->{type}'\n"
            if !defined($scfg->{path});

        my $content = $param->{content};

        my $tmpfilename = $param->{tmpfilename};
        die "missing temporary file name\n" if !$tmpfilename;

        my $size = -s $tmpfilename;
        die "temporary file '$tmpfilename' does not exist\n" if !defined($size);

        my $filename = PVE::Storage::normalize_content_filename($param->{filename});

        my $path;
        my $is_ova = 0;
        my $image_format;

        if ($content eq 'iso') {
            if ($filename !~ m![^/]+$PVE::Storage::ISO_EXT_RE_0$!) {
                raise_param_exc({ filename => "wrong file extension" });
            }
            $path = PVE::Storage::get_iso_dir($cfg, $storage);
        } elsif ($content eq 'vztmpl') {
            if ($filename !~ m![^/]+$PVE::Storage::VZTMPL_EXT_RE_1$!) {
                raise_param_exc({ filename => "wrong file extension" });
            }
            $path = PVE::Storage::get_vztmpl_dir($cfg, $storage);
        } elsif ($content eq 'import') {
            if ($filename !~
                m!${PVE::Storage::SAFE_CHAR_CLASS_RE}+$PVE::Storage::UPLOAD_IMPORT_EXT_RE_1$!
            ) {
                raise_param_exc({ filename => "invalid filename or wrong extension" });
            }
            my $format = $1;

            if ($format eq 'ova') {
                $is_ova = 1;
            } else {
                $image_format = $format;
            }

            $path = PVE::Storage::get_import_dir($cfg, $storage);
        } else {
            raise_param_exc({ content => "upload content type '$content' not allowed" });
        }

        die "storage '$storage' does not support '$content' content\n"
            if !$scfg->{content}->{$content};

        my $dest = "$path/$filename";
        my $dirname = dirname($dest);

        # best effort to match apl_download behaviour
        chmod 0644, $tmpfilename;

        my $err_cleanup = sub { unlink $dest or $! == ENOENT or die "cleanup failed: $!\n" };

        my $cmd;
        if ($node ne 'localhost' && $node ne PVE::INotify::nodename()) {
            my $remip = PVE::Cluster::remote_node_ip($node);

            my $ssh_options =
                PVE::SSHInfo::ssh_info_to_ssh_opts({ ip => $remip, name => $node });

            my @remcmd = ('/usr/bin/ssh', $ssh_options->@*, $remip, '--');

            eval { # activate remote storage
                run_command([@remcmd, '/usr/sbin/pvesm', 'status', '--storage', $storage]);
            };
            die "can't activate storage '$storage' on node '$node': $@\n" if $@;

            run_command(
                [@remcmd, '/bin/mkdir', '-p', '--', PVE::Tools::shell_quote($dirname)],
                errmsg => "mkdir failed",
            );

            $cmd = [
                '/usr/bin/scp',
                $ssh_options->@*,
                '-p',
                '--',
                $tmpfilename,
                "[$remip]:" . PVE::Tools::shell_quote($dest),
            ];

            $err_cleanup = sub { run_command([@remcmd, 'rm', '-f', '--', $dest]) };
        } else {
            PVE::Storage::activate_storage($cfg, $storage);
            File::Path::make_path($dirname);
            $cmd = ['cp', '--', $tmpfilename, $dest];
        }

        # NOTE: we simply overwrite the destination file if it already exists
        my $worker = sub {
            my $upid = shift;

            print "starting file import from: $tmpfilename\n";

            eval {
                my ($checksum, $checksum_algorithm) =
                    $param->@{ 'checksum', 'checksum-algorithm' };
                if ($checksum_algorithm) {
                    print "calculating checksum...";

                    my $checksum_got =
                        PVE::Tools::get_file_hash($checksum_algorithm, $tmpfilename);

                    if (lc($checksum_got) eq lc($checksum)) {
                        print "OK, checksum verified\n";
                    } else {
                        print "\n"; # the front end expects the error to reside at the last line without any noise
                        die "checksum mismatch: got '$checksum_got' != expect '$checksum'\n";
                    }
                }

                if ($content eq 'iso') {
                    PVE::Storage::assert_iso_content($tmpfilename);
                }

                if ($is_ova) {
                    assert_ova_contents($tmpfilename);
                } elsif (defined($image_format)) {
                    # checks untrusted image
                    PVE::Storage::file_size_info($tmpfilename, 10, $image_format, 1);
                }
            };
            if (my $err = $@) {
                # unlinks only the temporary file from the http server
                unlink $tmpfilename
                    or $! == ENOENT
                    or warn "unable to clean up temporory file '$tmpfilename' - $!\n";
                die $err;
            }

            print "target node: $node\n";
            print "target file: $dest\n";
            print "file size is: $size\n";
            print "command: " . join(' ', @$cmd) . "\n";

            eval { run_command($cmd, errmsg => 'import failed'); };

            # the temporary file got only uploaded locally, no need to rm remote
            unlink $tmpfilename
                or $! == ENOENT
                or warn "unable to clean up temporary file '$tmpfilename' - $!\n";

            if (my $err = $@) {
                eval { $err_cleanup->() };
                warn "$@" if $@;
                die $err;
            }
            print "finished file import successfully\n";
        };

        return $rpcenv->fork_worker('imgcopy', undef, $user, $worker);
    },
});

__PACKAGE__->register_method({
    name => 'download_url',
    path => '{storage}/download-url',
    method => 'POST',
    description => "Download templates, ISO images, OVAs and VM images by using an URL.",
    proxyto => 'node',
    permissions => {
        description =>
            'Requires allocation access on the storage and as this allows one to probe'
            . ' the (local!) host network indirectly it also requires one of Sys.Modify on / (for'
            . ' backwards compatibility) or the newer Sys.AccessNetwork privilege on the node.',
        check => [
            'and',
            ['perm', '/storage/{storage}', ['Datastore.AllocateTemplate']],
            [
                'or',
                ['perm', '/', ['Sys.Audit', 'Sys.Modify']],
                ['perm', '/nodes/{node}', ['Sys.AccessNetwork']],
            ],
        ],
    },
    protected => 1,
    parameters => {
        additionalProperties => 0,
        properties => {
            node => get_standard_option('pve-node'),
            storage => get_standard_option('pve-storage-id'),
            url => {
                description => "The URL to download the file from.",
                type => 'string',
                pattern => 'https?://.*',
            },
            content => {
                description => "Content type.", # TODO: could be optional & detected in most cases
                type => 'string',
                format => 'pve-storage-content',
                enum => ['iso', 'vztmpl', 'import'],
            },
            filename => {
                description =>
                    "The name of the file to create. Caution: This will be normalized!",
                maxLength => 255,
                type => 'string',
            },
            checksum => {
                description => "The expected checksum of the file.",
                type => 'string',
                requires => 'checksum-algorithm',
                optional => 1,
            },
            compression => {
                description =>
                    "Decompress the downloaded file using the specified compression algorithm.",
                type => 'string',
                enum => $PVE::Storage::Plugin::KNOWN_COMPRESSION_FORMATS,
                optional => 1,
            },
            'checksum-algorithm' => {
                description => "The algorithm to calculate the checksum of the file.",
                type => 'string',
                enum => ['md5', 'sha1', 'sha224', 'sha256', 'sha384', 'sha512'],
                requires => 'checksum',
                optional => 1,
            },
            'verify-certificates' => {
                description => "If false, no SSL/TLS certificates will be verified.",
                type => 'boolean',
                optional => 1,
                default => 1,
            },
        },
    },
    returns => {
        type => "string",
    },
    code => sub {
        my ($param) = @_;

        my $rpcenv = PVE::RPCEnvironment::get();
        my $user = $rpcenv->get_user();

        my $cfg = PVE::Storage::config();

        my ($node, $storage, $compression) = $param->@{qw(node storage compression)};
        my $scfg = PVE::Storage::storage_check_enabled($cfg, $storage, $node);

        die "can't upload to storage type '$scfg->{type}', not a file based storage!\n"
            if !defined($scfg->{path});

        my ($content, $url) = $param->@{ 'content', 'url' };

        die "storage '$storage' is not configured for content-type '$content'\n"
            if !$scfg->{content}->{$content};

        my $filename = PVE::Storage::normalize_content_filename($param->{filename});

        my $path;
        my $is_ova = 0;
        my $image_format;

        if ($content eq 'iso') {
            if ($filename !~ m![^/]+$PVE::Storage::ISO_EXT_RE_0$!) {
                raise_param_exc({ filename => "wrong file extension" });
            }
            $path = PVE::Storage::get_iso_dir($cfg, $storage);
        } elsif ($content eq 'vztmpl') {
            if ($filename !~ m![^/]+$PVE::Storage::VZTMPL_EXT_RE_1$!) {
                raise_param_exc({ filename => "wrong file extension" });
            }
            $path = PVE::Storage::get_vztmpl_dir($cfg, $storage);
        } elsif ($content eq 'import') {
            if ($filename !~
                m!${PVE::Storage::SAFE_CHAR_CLASS_RE}+$PVE::Storage::UPLOAD_IMPORT_EXT_RE_1$!
            ) {
                raise_param_exc({ filename => "invalid filename or wrong extension" });
            }
            my $format = $1;

            if ($format eq 'ova') {
                $is_ova = 1;
            } else {
                $image_format = $format;
            }

            $path = PVE::Storage::get_import_dir($cfg, $storage);
        } else {
            raise_param_exc({ content => "upload content-type '$content' is not allowed" });
        }

        PVE::Storage::activate_storage($cfg, $storage);
        File::Path::make_path($path);

        my $dccfg = PVE::Cluster::cfs_read_file('datacenter.cfg');
        my $opts = {
            hash_required => 0,
            verify_certificates => $param->{'verify-certificates'} // 1,
            http_proxy => $dccfg->{http_proxy},
            https_proxy => $dccfg->{http_proxy},
        };

        my ($checksum, $checksum_algorithm) = $param->@{ 'checksum', 'checksum-algorithm' };
        if ($checksum) {
            $opts->{"${checksum_algorithm}sum"} = $checksum;
            $opts->{hash_required} = 1;
        }

        $opts->{assert_file_validity} = sub {
            my ($tmp_path) = @_;

            if ($content eq 'iso') {
                PVE::Storage::assert_iso_content($tmp_path);
            }

            if ($is_ova) {
                assert_ova_contents($tmp_path);
            } elsif (defined($image_format)) {
                # checks untrusted image
                PVE::Storage::file_size_info($tmp_path, 10, $image_format, 1);
            }
        };

        my $worker = sub {
            if ($compression) {
                die "decompression not supported for $content\n" if $content ne 'iso';
                my $info = PVE::Storage::decompressor_info('iso', $compression);
                die "no decompression method found\n" if !$info->{decompressor};
                $opts->{decompression_command} = $info->{decompressor};
            }

            PVE::Tools::download_file_from_url("$path/$filename", $url, $opts);
        };

        my $worker_id = PVE::Tools::encode_text($filename); # must not pass : or the like as w-ID

        return $rpcenv->fork_worker('download', $worker_id, $user, $worker);
    },
});

__PACKAGE__->register_method({
    name => 'get_import_metadata',
    path => '{storage}/import-metadata',
    method => 'GET',
    description =>
        "Get the base parameters for creating a guest which imports data from a foreign importable"
        . " guest, like an ESXi VM",
    proxyto => 'node',
    permissions => {
        description => "You need read access for the volume.",
        user => 'all',
    },
    protected => 1,
    parameters => {
        additionalProperties => 0,
        properties => {
            node => get_standard_option('pve-node'),
            storage => get_standard_option('pve-storage-id'),
            volume => {
                description => "Volume identifier for the guest archive/entry.",
                type => 'string',
            },
        },
    },
    returns => {
        type => "object",
        description => 'Information about how to import a guest.',
        additionalProperties => 0,
        properties => {
            type => {
                type => 'string',
                enum => ['vm'],
                description => 'The type of guest this is going to produce.',
            },
            source => {
                type => 'string',
                enum => ['esxi'],
                description => 'The type of the import-source of this guest volume.',
            },
            'create-args' => {
                type => 'object',
                additionalProperties => 1,
                description =>
                    'Parameters which can be used in a call to create a VM or container.',
            },
            'disks' => {
                type => 'object',
                additionalProperties => 1,
                optional => 1,
                description => 'Recognised disk volumes as `$bus$id` => `$storeid:$path` map.',
            },
            'net' => {
                type => 'object',
                additionalProperties => 1,
                optional => 1,
                description =>
                    'Recognised network interfaces as `net$id` => { ...params } object.',
            },
            'warnings' => {
                type => 'array',
                description => 'List of known issues that can affect the import of a guest.'
                    . ' Note that lack of warning does not imply that there cannot be any problems.',
                optional => 1,
                items => {
                    type => "object",
                    additionalProperties => 1,
                    properties => {
                        'type' => {
                            description => 'What this warning is about.',
                            enum => [
                                'cdrom-image-ignored',
                                'efi-state-lost',
                                'guest-is-running',
                                'nvme-unsupported',
                                'ova-needs-extracting',
                                'ovmf-with-lsi-unsupported',
                                'serial-port-socket-only',
                            ],
                            type => 'string',
                        },
                        'key' => {
                            description => 'Related subject (config) key of warning.',
                            optional => 1,
                            type => 'string',
                        },
                        'value' => {
                            description => 'Related subject (config) value of warning.',
                            optional => 1,
                            type => 'string',
                        },
                    },
                },
            },
        },
    },
    code => sub {
        my ($param) = @_;

        my $rpcenv = PVE::RPCEnvironment::get();
        my $authuser = $rpcenv->get_user();

        my ($storeid, $volume) = $param->@{qw(storage volume)};
        my $volid = "$storeid:$volume";

        my $cfg = PVE::Storage::config();

        PVE::Storage::check_volume_access($rpcenv, $authuser, $cfg, undef, $volid);

        return PVE::Tools::run_with_timeout(
            30,
            sub {
                return PVE::Storage::get_import_metadata($cfg, $volid);
            },
        );
    },
});

1;
