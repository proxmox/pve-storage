package PVE::Storage::TestVolnameForFormat;

use v5.36;

use lib qw(..);

use Test::More;

use PVE::Storage;
use PVE::Storage::Plugin;
use PVE::Storage::LVMPlugin;
use PVE::Storage::LvmThinPlugin;
use PVE::Storage::RBDPlugin;
use PVE::Storage::ZFSPoolPlugin;
use PVE::Storage::ZFSPlugin;
use PVE::Storage::BTRFSPlugin;

# Each case exercises volname_for_format() and declares exactly one of:
#   expect_pass       - must not die and must return the input name unchanged
#   expect_return     - must not die and must return this adapted name
#   expect_suggestion - must die and suggest this corrected name
#   expect_error      - must die with an error matching this regex
#
# The 'strict' flag selects the mode: 1 = strict (old verify_volname_format
# behavior), 0 = lax (default). A case without 'strict' verifies that the
# default mode is lax.
my $tests = [
    # ======================================================================
    # PVE::Storage::Plugin (base / dir-like behavior)
    # Every format carries a file extension, so a mismatch always spells out a
    # contradicting format and dies even in lax mode. An extension-less name is
    # adapted in lax mode and the result has to pass parse_name_dir.
    # ======================================================================

    # --- strict ---
    {
        desc => 'volname_for_format_strict_PluginMatchingRawFormat_Succeeds',
        class => 'PVE::Storage::Plugin',
        name => 'vm-100-disk-0.raw',
        fmt => 'raw',
        strict => 1,
        expect_pass => 1,
    },
    {
        desc => 'volname_for_format_strict_PluginMatchingQcow2Format_Succeeds',
        class => 'PVE::Storage::Plugin',
        name => 'vm-100-disk-0.qcow2',
        fmt => 'qcow2',
        strict => 1,
        expect_pass => 1,
    },
    {
        desc => 'volname_for_format_strict_PluginMatchingSubvolFormat_Succeeds',
        class => 'PVE::Storage::Plugin',
        name => 'subvol-100-disk-0.subvol',
        fmt => 'subvol',
        strict => 1,
        expect_pass => 1,
    },
    {
        desc => 'volname_for_format_strict_PluginMatchingVmdkFormat_Succeeds',
        class => 'PVE::Storage::Plugin',
        name => 'vm-100-disk-0.vmdk',
        fmt => 'vmdk',
        strict => 1,
        expect_pass => 1,
    },
    {
        desc => 'volname_for_format_strict_PluginMatchingBaseQcow2Format_Succeeds',
        class => 'PVE::Storage::Plugin',
        name => 'base-100-disk-0.qcow2',
        fmt => 'qcow2',
        strict => 1,
        expect_pass => 1,
    },
    {
        desc =>
            'volname_for_format_strict_PluginMismatchedRawNameWithQcow2Format_ThrowsSuggestionError',
        class => 'PVE::Storage::Plugin',
        name => 'vm-100-disk-0.raw',
        fmt => 'qcow2',
        strict => 1,
        expect_suggestion => 'vm-100-disk-0.qcow2',
    },
    {
        desc =>
            'volname_for_format_strict_PluginMismatchedQcow2NameWithRawFormat_ThrowsSuggestionError',
        class => 'PVE::Storage::Plugin',
        name => 'vm-100-disk-0.qcow2',
        fmt => 'raw',
        strict => 1,
        expect_suggestion => 'vm-100-disk-0.raw',
    },
    {
        desc =>
            'volname_for_format_strict_PluginMismatchedQcow2NameWithSubvolFormat_ThrowsSuggestionError',
        class => 'PVE::Storage::Plugin',
        name => 'vm-100-disk-0.qcow2',
        fmt => 'subvol',
        strict => 1,
        expect_suggestion => 'vm-100-disk-0.subvol',
    },
    {
        desc =>
            'volname_for_format_strict_PluginMismatchedBaseQcow2NameWithRawFormat_ThrowsSuggestionError',
        class => 'PVE::Storage::Plugin',
        name => 'base-100-disk-0.qcow2',
        fmt => 'raw',
        strict => 1,
        expect_suggestion => 'base-100-disk-0.raw',
    },
    {
        desc =>
            'volname_for_format_strict_PluginMismatchedMultiDotNameWithRawFormat_ThrowsSuggestionError',
        class => 'PVE::Storage::Plugin',
        name => 'vm-100-disk-0.backup.qcow2',
        fmt => 'raw',
        strict => 1,
        expect_suggestion => 'vm-100-disk-0.backup.raw',
    },
    # --- lax ---
    {
        desc => 'volname_for_format_lax_PluginMatchingRawFormat_Succeeds',
        class => 'PVE::Storage::Plugin',
        name => 'vm-100-disk-0.raw',
        fmt => 'raw',
        strict => 0,
        expect_pass => 1,
    },
    {
        desc =>
            'volname_for_format_lax_PluginMismatchedRawNameWithQcow2Format_ThrowsSuggestionError',
        class => 'PVE::Storage::Plugin',
        name => 'vm-100-disk-0.raw',
        fmt => 'qcow2',
        strict => 0,
        expect_suggestion => 'vm-100-disk-0.qcow2',
    },
    {
        desc => 'volname_for_format_lax_PluginExtensionlessNameWithQcow2Format_AdaptsName',
        class => 'PVE::Storage::Plugin',
        name => 'vm-100-cloudinit',
        fmt => 'qcow2',
        strict => 0,
        expect_return => 'vm-100-cloudinit.qcow2',
    },
    {
        desc => 'volname_for_format_lax_PluginExtensionlessNameWithRawFormat_AdaptsName',
        class => 'PVE::Storage::Plugin',
        name => 'vm-100-cloudinit',
        fmt => 'raw',
        strict => 0,
        expect_return => 'vm-100-cloudinit.raw',
    },
    {
        # an unknown extension must keep failing in parse_name_dir instead of being taken as raw
        desc => 'volname_for_format_lax_PluginUnknownExtensionWithRawFormat_ThrowsParsingError',
        class => 'PVE::Storage::Plugin',
        name => 'vm-100-disk-0.vhdx',
        fmt => 'raw',
        strict => 0,
        expect_error => qr/unable to parse volume filename/,
    },
    {
        # an adapted name must go through the same checks as a given one, or a name without an
        # extension could smuggle path separators past parse_name_dir
        desc => 'volname_for_format_lax_PluginExtensionlessNameWithSlash_ThrowsParsingError',
        class => 'PVE::Storage::Plugin',
        name => 'vm-100-disk-0/../escape',
        fmt => 'raw',
        strict => 0,
        expect_error => qr/unable to parse volume filename/,
    },
    {
        desc =>
            'volname_for_format_lax_PluginExtensionlessNameWithWhitespace_ThrowsParsingError',
        class => 'PVE::Storage::Plugin',
        name => 'vm-100-disk 0',
        fmt => 'raw',
        strict => 0,
        expect_error => qr/unable to parse volume filename/,
    },
    {
        desc => 'volname_for_format_lax_PluginTrailingDotTraversalName_ThrowsParsingError',
        class => 'PVE::Storage::Plugin',
        name => '../../escape.',
        fmt => 'raw',
        strict => 0,
        expect_error => qr/unable to parse volume filename/,
    },
    {
        desc => 'volname_for_format_lax_PluginExtensionlessSnapshotName_ThrowsSnapshotError',
        class => 'PVE::Storage::Plugin',
        name => 'snap-vm-100-disk-0',
        fmt => 'qcow2',
        strict => 0,
        expect_error => qr/is a snapshot filename/,
    },
    # ======================================================================
    # PVE::Storage::LVMPlugin
    # Raw volumes have no extension, so a raw-parsed name does not spell out a
    # format and lax mode adapts it. A .qcow2 name does spell out a format and
    # stays an error in both modes.
    # ======================================================================

    # --- strict ---
    {
        desc => 'volname_for_format_strict_LvmMatchingRawFormat_Succeeds',
        class => 'PVE::Storage::LVMPlugin',
        name => 'vm-100-disk-0',
        fmt => 'raw',
        strict => 1,
        expect_pass => 1,
    },
    {
        desc => 'volname_for_format_strict_LvmMatchingQcow2Format_Succeeds',
        class => 'PVE::Storage::LVMPlugin',
        name => 'vm-100-disk-0.qcow2',
        fmt => 'qcow2',
        strict => 1,
        expect_pass => 1,
    },
    {
        desc =>
            'volname_for_format_strict_LvmMismatchedRawNameWithQcow2Format_ThrowsSuggestionError',
        class => 'PVE::Storage::LVMPlugin',
        name => 'vm-100-disk-0',
        fmt => 'qcow2',
        strict => 1,
        expect_suggestion => 'vm-100-disk-0.qcow2',
    },
    {
        desc =>
            'volname_for_format_strict_LvmMismatchedQcow2NameWithRawFormat_ThrowsSuggestionError',
        class => 'PVE::Storage::LVMPlugin',
        name => 'vm-100-disk-0.qcow2',
        fmt => 'raw',
        strict => 1,
        expect_suggestion => 'vm-100-disk-0',
    },
    {
        desc => 'volname_for_format_strict_LvmNonFormatSuffix_Succeeds',
        class => 'PVE::Storage::LVMPlugin',
        name => 'vm-100-disk-0.disk',
        fmt => 'raw',
        strict => 1,
        expect_pass => 1,
    },
    {
        desc => 'volname_for_format_strict_LvmInvalidName_ThrowsParsingError',
        class => 'PVE::Storage::LVMPlugin',
        name => 'invalid-name',
        fmt => 'raw',
        strict => 1,
        expect_error => qr/unable to parse lvm volume name/,
    },
    {
        desc => 'volname_for_format_strict_LvmRawSuffixWithRawFormat_Succeeds',
        class => 'PVE::Storage::LVMPlugin',
        name => 'vm-100-disk-0.raw',
        fmt => 'raw',
        strict => 1,
        expect_pass => 1,
    },

    # --- lax ---
    {
        desc => 'volname_for_format_lax_LvmMatchingRawFormat_Succeeds',
        class => 'PVE::Storage::LVMPlugin',
        name => 'vm-100-disk-0',
        fmt => 'raw',
        strict => 0,
        expect_pass => 1,
    },
    {
        desc => 'volname_for_format_lax_LvmMatchingQcow2Format_Succeeds',
        class => 'PVE::Storage::LVMPlugin',
        name => 'vm-100-disk-0.qcow2',
        fmt => 'qcow2',
        strict => 0,
        expect_pass => 1,
    },
    {
        desc => 'volname_for_format_lax_LvmExtensionlessNameWithQcow2Format_AdaptsName',
        class => 'PVE::Storage::LVMPlugin',
        name => 'vm-100-cloudinit',
        fmt => 'qcow2',
        strict => 0,
        expect_return => 'vm-100-cloudinit.qcow2',
    },
    {
        desc =>
            'volname_for_format_lax_LvmMismatchedQcow2NameWithRawFormat_ThrowsSuggestionError',
        class => 'PVE::Storage::LVMPlugin',
        name => 'vm-100-disk-0.qcow2',
        fmt => 'raw',
        strict => 0,
        expect_suggestion => 'vm-100-disk-0',
    },
    {
        desc => 'volname_for_format_lax_LvmRawSuffixWithRawFormat_Succeeds',
        class => 'PVE::Storage::LVMPlugin',
        name => 'vm-100-disk-0.raw',
        fmt => 'raw',
        strict => 0,
        expect_pass => 1,
    },
    {
        # .raw is not a real LVM extension (raw has none), so it is treated as a
        # stray suffix, stripped, and replaced by the requested extension.
        desc => 'volname_for_format_lax_LvmRawSuffixWithQcow2Format_ThrowsSuggestionError',
        class => 'PVE::Storage::LVMPlugin',
        name => 'vm-100-disk-0.raw',
        fmt => 'qcow2',
        strict => 0,
        expect_suggestion => 'vm-100-disk-0.qcow2',
    },
    {
        desc => 'volname_for_format_lax_LvmNonFormatSuffixWithRawFormat_Succeeds',
        class => 'PVE::Storage::LVMPlugin',
        name => 'vm-100-disk-0.disk',
        fmt => 'raw',
        strict => 0,
        expect_pass => 1,
    },
    {
        desc =>
            'volname_for_format_lax_LvmNonFormatSuffixWithQcow2Format_ThrowsSuggestionError',
        class => 'PVE::Storage::LVMPlugin',
        name => 'vm-100-disk-0.disk',
        fmt => 'qcow2',
        strict => 0,
        expect_suggestion => 'vm-100-disk-0.qcow2',
    },
    {
        desc => 'volname_for_format_lax_LvmInvalidName_ThrowsParsingError',
        class => 'PVE::Storage::LVMPlugin',
        name => 'invalid-name',
        fmt => 'raw',
        strict => 0,
        expect_error => qr/unable to parse lvm volume name/,
    },
    {
        desc => 'volname_for_format_lax_LvmTraversalName_ThrowsParsingError',
        class => 'PVE::Storage::LVMPlugin',
        name => '../vm-100-disk-0',
        fmt => 'raw',
        strict => 0,
        expect_error => qr/contains illegal characters/,
    },
    {
        desc => 'volname_for_format_lax_LvmUnsupportedFormat_ThrowsError',
        class => 'PVE::Storage::LVMPlugin',
        name => 'vm-100-disk-0',
        fmt => 'vmdk',
        strict => 0,
        expect_error => qr/unsupported format 'vmdk'/,
    },

    # ======================================================================
    # PVE::Storage::LvmThinPlugin
    # ======================================================================

    # --- strict ---
    {
        desc => 'volname_for_format_strict_LvmThinMatchingRawFormat_Succeeds',
        class => 'PVE::Storage::LvmThinPlugin',
        name => 'vm-100-disk-0',
        fmt => 'raw',
        strict => 1,
        expect_pass => 1,
    },
    {
        desc =>
            'volname_for_format_strict_LvmThinMismatchedQcow2NameWithRawFormat_ThrowsSuggestionError',
        class => 'PVE::Storage::LvmThinPlugin',
        name => 'vm-100-disk-0.qcow2',
        fmt => 'raw',
        strict => 1,
        expect_suggestion => 'vm-100-disk-0',
    },
    {
        desc => 'volname_for_format_strict_LvmThinRawSuffixWithRawFormat_Succeeds',
        class => 'PVE::Storage::LvmThinPlugin',
        name => 'vm-100-disk-0.raw',
        fmt => 'raw',
        strict => 1,
        expect_pass => 1,
    },
    {
        desc => 'volname_for_format_strict_LvmThinNonFormatSuffix_Succeeds',
        class => 'PVE::Storage::LvmThinPlugin',
        name => 'vm-100-disk-0.disk',
        fmt => 'raw',
        strict => 1,
        expect_pass => 1,
    },
    {
        desc => 'volname_for_format_strict_LvmThinInvalidName_ThrowsParsingError',
        class => 'PVE::Storage::LvmThinPlugin',
        name => 'invalid-name',
        fmt => 'raw',
        strict => 1,
        expect_error => qr/unable to parse lvm volume name/,
    },
    # --- lax ---
    {
        desc => 'volname_for_format_lax_LvmThinMatchingRawFormat_Succeeds',
        class => 'PVE::Storage::LvmThinPlugin',
        name => 'vm-100-disk-0',
        fmt => 'raw',
        strict => 0,
        expect_pass => 1,
    },
    {
        desc =>
            'volname_for_format_lax_LvmThinMismatchedQcow2NameWithRawFormat_ThrowsSuggestionError',
        class => 'PVE::Storage::LvmThinPlugin',
        name => 'vm-100-disk-0.qcow2',
        fmt => 'raw',
        strict => 0,
        expect_suggestion => 'vm-100-disk-0',
    },
    {
        desc => 'volname_for_format_lax_LvmThinRawSuffixWithRawFormat_Succeeds',
        class => 'PVE::Storage::LvmThinPlugin',
        name => 'vm-100-disk-0.raw',
        fmt => 'raw',
        strict => 0,
        expect_pass => 1,
    },
    {
        desc => 'volname_for_format_lax_LvmThinInvalidName_ThrowsParsingError',
        class => 'PVE::Storage::LvmThinPlugin',
        name => 'invalid-name',
        fmt => 'raw',
        strict => 0,
        expect_error => qr/unable to parse lvm volume name/,
    },

    # ======================================================================
    # PVE::Storage::RBDPlugin
    # ======================================================================

    # --- strict ---
    {
        desc => 'volname_for_format_strict_RbdMatchingRawFormat_Succeeds',
        class => 'PVE::Storage::RBDPlugin',
        name => 'vm-100-disk-0',
        fmt => 'raw',
        strict => 1,
        expect_pass => 1,
    },
    {
        desc =>
            'volname_for_format_strict_RbdMismatchedQcow2NameWithRawFormat_ThrowsSuggestionError',
        class => 'PVE::Storage::RBDPlugin',
        name => 'vm-100-disk-0.qcow2',
        fmt => 'raw',
        strict => 1,
        expect_suggestion => 'vm-100-disk-0',
    },
    {
        desc => 'volname_for_format_strict_RbdRawSuffixWithRawFormat_Succeeds',
        class => 'PVE::Storage::RBDPlugin',
        name => 'vm-100-disk-0.raw',
        fmt => 'raw',
        strict => 1,
        expect_pass => 1,
    },
    {
        desc => 'volname_for_format_strict_RbdNonFormatSuffix_Succeeds',
        class => 'PVE::Storage::RBDPlugin',
        name => 'vm-100-disk-0.disk',
        fmt => 'raw',
        strict => 1,
        expect_pass => 1,
    },
    {
        desc => 'volname_for_format_strict_RbdInvalidName_ThrowsParsingError',
        class => 'PVE::Storage::RBDPlugin',
        name => 'invalid-name',
        fmt => 'raw',
        strict => 1,
        expect_error => qr/unable to parse rbd volume name/,
    },

    # --- lax ---
    {
        desc => 'volname_for_format_lax_RbdMatchingRawFormat_Succeeds',
        class => 'PVE::Storage::RBDPlugin',
        name => 'vm-100-disk-0',
        fmt => 'raw',
        strict => 0,
        expect_pass => 1,
    },
    {
        desc =>
            'volname_for_format_lax_RbdMismatchedQcow2NameWithRawFormat_ThrowsSuggestionError',
        class => 'PVE::Storage::RBDPlugin',
        name => 'vm-100-disk-0.qcow2',
        fmt => 'raw',
        strict => 0,
        expect_suggestion => 'vm-100-disk-0',
    },
    {
        desc => 'volname_for_format_lax_RbdQcow2Format_ThrowsUnsupportedFormatError',
        class => 'PVE::Storage::RBDPlugin',
        name => 'vm-100-disk-0',
        fmt => 'qcow2',
        strict => 0,
        expect_error => qr/unsupported format 'qcow2'/,
    },
    {
        desc => 'volname_for_format_lax_LvmThinQcow2Format_ThrowsUnsupportedFormatError',
        class => 'PVE::Storage::LvmThinPlugin',
        name => 'vm-100-disk-0',
        fmt => 'qcow2',
        strict => 0,
        expect_error => qr/unsupported format 'qcow2'/,
    },
    {
        desc => 'volname_for_format_lax_RbdInvalidName_ThrowsParsingError',
        class => 'PVE::Storage::RBDPlugin',
        name => 'invalid-name',
        fmt => 'raw',
        strict => 0,
        expect_error => qr/unable to parse rbd volume name/,
    },

    # ======================================================================
    # PVE::Storage::ZFSPoolPlugin
    # The name prefix always spells out the format, so any mismatch is an error
    # in both modes.
    # ======================================================================

    # --- strict ---
    {
        desc => 'volname_for_format_strict_ZfsPoolMatchingRawFormat_Succeeds',
        class => 'PVE::Storage::ZFSPoolPlugin',
        name => 'vm-100-disk-0',
        fmt => 'raw',
        strict => 1,
        expect_pass => 1,
    },
    {
        desc => 'volname_for_format_strict_ZfsPoolMatchingSubvolFormat_Succeeds',
        class => 'PVE::Storage::ZFSPoolPlugin',
        name => 'subvol-100-disk-0',
        fmt => 'subvol',
        strict => 1,
        expect_pass => 1,
    },
    {
        desc => 'volname_for_format_strict_ZfsPoolMatchingBaseRawFormat_Succeeds',
        class => 'PVE::Storage::ZFSPoolPlugin',
        name => 'base-100-disk-0',
        fmt => 'raw',
        strict => 1,
        expect_pass => 1,
    },
    {
        desc => 'volname_for_format_strict_ZfsPoolMatchingBasevolSubvolFormat_Succeeds',
        class => 'PVE::Storage::ZFSPoolPlugin',
        name => 'basevol-100-disk-0',
        fmt => 'subvol',
        strict => 1,
        expect_pass => 1,
    },
    {
        desc =>
            'volname_for_format_strict_ZfsPoolMismatchedRawNameWithSubvolFormat_ThrowsSuggestionError',
        class => 'PVE::Storage::ZFSPoolPlugin',
        name => 'vm-100-disk-0',
        fmt => 'subvol',
        strict => 1,
        expect_suggestion => 'subvol-100-disk-0',
    },
    {
        desc =>
            'volname_for_format_strict_ZfsPoolMismatchedSubvolNameWithRawFormat_ThrowsSuggestionError',
        class => 'PVE::Storage::ZFSPoolPlugin',
        name => 'subvol-100-disk-0',
        fmt => 'raw',
        strict => 1,
        expect_suggestion => 'vm-100-disk-0',
    },
    {
        desc =>
            'volname_for_format_strict_ZfsPoolMismatchedBaseNameWithSubvolFormat_ThrowsSuggestionError',
        class => 'PVE::Storage::ZFSPoolPlugin',
        name => 'base-100-disk-0',
        fmt => 'subvol',
        strict => 1,
        expect_suggestion => 'basevol-100-disk-0',
    },
    {
        desc =>
            'volname_for_format_strict_ZfsPoolMismatchedBasevolNameWithRawFormat_ThrowsSuggestionError',
        class => 'PVE::Storage::ZFSPoolPlugin',
        name => 'basevol-100-disk-0',
        fmt => 'raw',
        strict => 1,
        expect_suggestion => 'base-100-disk-0',
    },
    {
        desc => 'volname_for_format_strict_ZfsPoolInvalidName_ThrowsParsingError',
        class => 'PVE::Storage::ZFSPoolPlugin',
        name => 'invalid-name',
        fmt => 'raw',
        strict => 1,
        expect_error => qr/unable to parse zfs volume name/,
    },

    # --- lax ---
    {
        desc => 'volname_for_format_lax_ZfsPoolMatchingRawFormat_Succeeds',
        class => 'PVE::Storage::ZFSPoolPlugin',
        name => 'vm-100-disk-0',
        fmt => 'raw',
        strict => 0,
        expect_pass => 1,
    },
    {
        # The prefix spells out the format, so lax mode still rejects the
        # mismatch.
        desc =>
            'volname_for_format_lax_ZfsPoolMismatchedRawNameWithSubvolFormat_ThrowsSuggestionError',
        class => 'PVE::Storage::ZFSPoolPlugin',
        name => 'vm-100-disk-0',
        fmt => 'subvol',
        strict => 0,
        expect_suggestion => 'subvol-100-disk-0',
    },
    {
        desc =>
            'volname_for_format_lax_ZfsPoolMismatchedSubvolNameWithRawFormat_ThrowsSuggestionError',
        class => 'PVE::Storage::ZFSPoolPlugin',
        name => 'subvol-100-disk-0',
        fmt => 'raw',
        strict => 0,
        expect_suggestion => 'vm-100-disk-0',
    },

    # ======================================================================
    # PVE::Storage::ZFSPlugin
    # ZFS over iSCSI only has raw zvols, so subvol is rejected as unsupported.
    # ======================================================================

    # --- strict ---
    {
        desc => 'volname_for_format_strict_ZfsMatchingRawFormat_Succeeds',
        class => 'PVE::Storage::ZFSPlugin',
        name => 'vm-100-disk-0',
        fmt => 'raw',
        strict => 1,
        expect_pass => 1,
    },
    {
        desc => 'volname_for_format_strict_ZfsSubvolFormat_ThrowsUnsupportedFormatError',
        class => 'PVE::Storage::ZFSPlugin',
        name => 'subvol-100-disk-0',
        fmt => 'subvol',
        strict => 1,
        expect_error => qr/unsupported format 'subvol'/,
    },

    # --- lax ---
    {
        desc => 'volname_for_format_lax_ZfsMatchingRawFormat_Succeeds',
        class => 'PVE::Storage::ZFSPlugin',
        name => 'vm-100-disk-0',
        fmt => 'raw',
        strict => 0,
        expect_pass => 1,
    },
    {
        desc => 'volname_for_format_lax_ZfsSubvolFormat_ThrowsUnsupportedFormatError',
        class => 'PVE::Storage::ZFSPlugin',
        name => 'vm-100-disk-0',
        fmt => 'subvol',
        strict => 0,
        expect_error => qr/unsupported format 'subvol'/,
    },

    # ======================================================================
    # PVE::Storage::BTRFSPlugin
    # All formats carry an extension, so like the base plugin a mismatch always
    # spells out a contradicting format.
    # ======================================================================

    # --- strict ---
    {
        desc => 'volname_for_format_strict_BtrfsMatchingRawFormat_Succeeds',
        class => 'PVE::Storage::BTRFSPlugin',
        name => 'vm-100-disk-0.raw',
        fmt => 'raw',
        strict => 1,
        expect_pass => 1,
    },
    {
        desc => 'volname_for_format_strict_BtrfsMatchingSubvolFormat_Succeeds',
        class => 'PVE::Storage::BTRFSPlugin',
        name => 'vm-100-disk-0.subvol',
        fmt => 'subvol',
        strict => 1,
        expect_pass => 1,
    },
    {
        desc => 'volname_for_format_strict_BtrfsMatchingQcow2Format_Succeeds',
        class => 'PVE::Storage::BTRFSPlugin',
        name => 'vm-100-disk-0.qcow2',
        fmt => 'qcow2',
        strict => 1,
        expect_pass => 1,
    },
    {
        desc =>
            'volname_for_format_strict_BtrfsMismatchedRawNameWithSubvolFormat_ThrowsSuggestionError',
        class => 'PVE::Storage::BTRFSPlugin',
        name => 'vm-100-disk-0.raw',
        fmt => 'subvol',
        strict => 1,
        expect_suggestion => 'vm-100-disk-0.subvol',
    },
    {
        desc =>
            'volname_for_format_strict_BtrfsMismatchedSubvolNameWithRawFormat_ThrowsSuggestionError',
        class => 'PVE::Storage::BTRFSPlugin',
        name => 'vm-100-disk-0.subvol',
        fmt => 'raw',
        strict => 1,
        expect_suggestion => 'vm-100-disk-0.raw',
    },
    {
        desc =>
            'volname_for_format_strict_BtrfsMismatchedQcow2NameWithRawFormat_ThrowsSuggestionError',
        class => 'PVE::Storage::BTRFSPlugin',
        name => 'vm-100-disk-0.qcow2',
        fmt => 'raw',
        strict => 1,
        expect_suggestion => 'vm-100-disk-0.raw',
    },
    {
        desc => 'volname_for_format_strict_BtrfsMatchingSubvolPrefixFormat_Succeeds',
        class => 'PVE::Storage::BTRFSPlugin',
        name => 'subvol-100-disk-0.subvol',
        fmt => 'subvol',
        strict => 1,
        expect_pass => 1,
    },

    # --- lax ---
    {
        desc => 'volname_for_format_lax_BtrfsMatchingRawFormat_Succeeds',
        class => 'PVE::Storage::BTRFSPlugin',
        name => 'vm-100-disk-0.raw',
        fmt => 'raw',
        strict => 0,
        expect_pass => 1,
    },
    {
        desc => 'volname_for_format_lax_BtrfsMatchingSubvolFormat_Succeeds',
        class => 'PVE::Storage::BTRFSPlugin',
        name => 'vm-100-disk-0.subvol',
        fmt => 'subvol',
        strict => 0,
        expect_pass => 1,
    },
    {
        desc =>
            'volname_for_format_lax_BtrfsMismatchedRawNameWithSubvolFormat_ThrowsSuggestionError',
        class => 'PVE::Storage::BTRFSPlugin',
        name => 'vm-100-disk-0.raw',
        fmt => 'subvol',
        strict => 0,
        expect_suggestion => 'vm-100-disk-0.subvol',
    },
    {
        desc => 'volname_for_format_lax_BtrfsExtensionlessNameWithSubvolFormat_AdaptsName',
        class => 'PVE::Storage::BTRFSPlugin',
        name => 'vm-100-cloudinit',
        fmt => 'subvol',
        strict => 0,
        expect_return => 'vm-100-cloudinit.subvol',
    },
    {
        desc => 'volname_for_format_lax_BtrfsExtensionlessNameWithSlash_ThrowsParsingError',
        class => 'PVE::Storage::BTRFSPlugin',
        name => 'vm-100-disk-0/../escape',
        fmt => 'raw',
        strict => 0,
        expect_error => qr/unable to parse volume filename/,
    },
];

sub assert_pass {
    my ($desc, $result, $err, $expected_name) = @_;
    if ($err) {
        fail($desc);
        diag("Unexpected error: $err");
    } elsif (!defined($result) || $result ne $expected_name) {
        fail($desc);
        diag("Returned '" . ($result // 'undef') . "' but expected '$expected_name'");
    } else {
        pass($desc);
    }
}

sub assert_return {
    my ($desc, $result, $err, $expected_return) = @_;
    if ($err) {
        fail($desc);
        diag("Unexpected error: $err");
    } elsif (!defined($result) || $result ne $expected_return) {
        fail($desc);
        diag("Returned '" . ($result // 'undef') . "' but expected '$expected_return'");
    } else {
        pass($desc);
    }
}

sub assert_suggestion {
    my ($desc, $name, $fmt, $suggestion, $err) = @_;
    my $expected_msg = "illegal name $name - volume name does not match requested format "
        . "'$fmt' (did you mean '$suggestion'?)";

    like($err, qr/\Q$expected_msg\E/, $desc);
    if (!$err || index($err, $expected_msg) == -1) {
        diag("Got: " . ($err || 'no error'));
    }
}

sub assert_error {
    my ($desc, $err, $pattern) = @_;
    like($err, $pattern, $desc);
    if (!$err || $err !~ $pattern) {
        diag("Got: " . ($err || 'no error'));
    }
}

sub run_case($case) {
    my $desc = $case->{desc};
    my $strict = $case->{strict};

    my $result;
    my $err = '';
    eval { $result = $case->{class}->volname_for_format($case->{name}, $case->{fmt}, $strict); };
    $err = $@ if $@;

    if ($case->{expect_pass}) {
        assert_pass($desc, $result, $err, $case->{name});
    } elsif (defined($case->{expect_return})) {
        assert_return($desc, $result, $err, $case->{expect_return});
    } elsif (defined($case->{expect_suggestion})) {
        assert_suggestion($desc, $case->{name}, $case->{fmt}, $case->{expect_suggestion}, $err);
    } elsif (defined($case->{expect_error})) {
        assert_error($desc, $err, $case->{expect_error});
    } else {
        fail($desc);
        diag('Test case has no expectation set');
    }
}

sub main {
    plan(tests => scalar($tests->@*));

    for my $case ($tests->@*) {
        run_case($case);
    }

    done_testing();
}

main();

1;
