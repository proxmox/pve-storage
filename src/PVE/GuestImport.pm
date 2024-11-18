package PVE::GuestImport;

use strict;
use warnings;

use File::Path;

use PVE::Storage;
use PVE::Tools qw(run_command);

sub extract_disk_from_import_file {
    my ($volid, $vmid, $target_storeid) = @_;

    my ($source_storeid, $volname) = PVE::Storage::parse_volume_id($volid);
    $target_storeid //= $source_storeid;
    my $cfg = PVE::Storage::config();

    my ($vtype, $name, undef, undef, undef, undef, $fmt) =
	PVE::Storage::parse_volname($cfg, $volid);

    die "only files with content type 'import' can be extracted\n"
	if $vtype ne 'import';

    die "only files from 'ova' format can be extracted\n"
	if $fmt !~ m/^ova\+/;

    # extract the inner file from the name
    my $archive_volid;
    my $inner_file;
    my $inner_fmt;
    if ($name =~ m!^(.*\.ova)/(${PVE::Storage::SAFE_CHAR_CLASS_RE}+)$!) {
	$archive_volid = "$source_storeid:import/$1";
	$inner_file = $2;
	($inner_fmt) = $fmt =~ /^ova\+(.*)$/;
    } else {
	die "cannot extract $volid - invalid volname $volname\n";
    }

    my $ova_path = PVE::Storage::path($cfg, $archive_volid);

    my $tmpdir = PVE::Storage::get_image_dir($cfg, $target_storeid, $vmid);
    my $pid = $$;
    $tmpdir .= "/tmp_${pid}_${vmid}";
    mkpath $tmpdir;

    my $source_path = "$tmpdir/$inner_file";
    my $target_path;
    my $target_volid;
    eval {
	run_command(['tar', '-x', '--force-local', '-C', $tmpdir, '-f', $ova_path, $inner_file]);

	# check for symlinks and other non regular files
	if (-l $source_path || ! -f $source_path) {
	    die "extracted file '$inner_file' from archive '$archive_volid' is not a regular file\n";
	}

	# check potentially untrusted image file!
	PVE::Storage::file_size_info($source_path, undef, 1);

	# create temporary 1M image that will get overwritten by the rename
	# to reserve the filename and take care of locking
	$target_volid = PVE::Storage::vdisk_alloc($cfg, $target_storeid, $vmid, $inner_fmt, undef, 1024);
	$target_path = PVE::Storage::path($cfg, $target_volid);

	print "renaming $source_path to $target_path\n";

	rename($source_path, $target_path) or die "unable to move - $!\n";
    };
    if (my $err = $@) {
	File::Path::remove_tree($tmpdir);
	die "error during extraction: $err\n";
    }

    File::Path::remove_tree($tmpdir);

    return $target_volid;
}

1;
