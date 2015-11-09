package PVE::Storage::LvmThinPlugin;

use strict;
use warnings;
use IO::File;
use PVE::Tools qw(run_command trim);
use PVE::Storage::Plugin;
use PVE::JSONSchema qw(get_standard_option);

# see: man lvmthin

use base qw(PVE::Storage::LVMPlugin);

sub type {
    return 'lvmthin';
}

sub plugindata {
    return {
	content => [ {images => 1, rootdir => 1}, { images => 1, rootdir => 1}],
    };
}

sub properties {
    return {
	thinpool => {
	    description => "LVM thin pool LV name.",
	    type => 'string', format => 'pve-storage-vgname',
	},
    };
}

sub options {
    return {
	thinpool => { fixed => 1 },
	vgname => { fixed => 1 },
        nodes => { optional => 1 },
	disable => { optional => 1 },
	content => { optional => 1 },
    };
}

sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;

    die "unsupported format '$fmt'" if $fmt ne 'raw';

    die "illegal name '$name' - sould be 'vm-$vmid-*'\n" 
	if  $name && $name !~ m/^vm-$vmid-/;

    die "implement me";
}

sub free_image {
    my ($class, $storeid, $scfg, $volname, $isBase) = @_;

    my $vg = $scfg->{vgname};
    my $cmd = ['/sbin/lvremove', '-f', "$vg/$volname"];
    run_command($cmd, errmsg => "lvremove '$vg/$volname' error");

    return undef;
}

sub list_images {
    my ($class, $storeid, $scfg, $vmid, $vollist, $cache) = @_;

    die "implement me";
}

sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    my $lvname = "$scfg->{vgname}/$scfg->{thinpool}";
    
    my $cmd = ['/sbin/lvs', '--separator', ':', '--noheadings', '--units', 'b',
	       '--unbuffered', '--nosuffix', '--options',
	       'vg_name,lv_name,lv_size,data_percent,metadata_percent,snap_percent', $lvname];

    my $total = 0;
    my $used = 0;
    
    run_command($cmd, outfunc => sub {
	my $line = shift;

	$line = trim($line);

	my ($vg, $lv, $size, $data_percent, $meta_percent, $snap_percent) = split(':', $line);

	return if !$vg || $vg ne $scfg->{vgname};
	return if !$lv || $lv ne $scfg->{thinpool};
	
	$data_percent ||= 0;
	$meta_percent ||= 0;
	$snap_percent ||= 0;
		
	$total = $size;
	$used = int((($data_percent + $meta_percent + $snap_percent) * $size)/100)
    });
    
    return ($total, $total - $used, $used, 1) if $total;

    return undef;
}

sub activate_volume {
    my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;
}

sub deactivate_volume {
    my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;
}

sub volume_resize {
    my ($class, $scfg, $storeid, $volname, $size, $running) = @_;
    die "implement me";
}

sub volume_has_feature {
    my ($class, $scfg, $feature, $storeid, $volname, $snapname, $running) = @_;

    my $features = {
	copy => { base => 1, current => 1},
    };

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase) =
	$class->parse_volname($volname);

    my $key = undef;
    if($snapname){
	$key = 'snap';
    }else{
	$key =  $isBase ? 'base' : 'current';
    }
    return 1 if $features->{$feature}->{$key};

    return undef;
}

1;
