# copyright, 2014 Terascala, Inc. All rights reserved
#
# lustreClient may be copied only under the terms of either the 
# Artistic License or the GNU General Public License

# Lustre Client Data Collector

use strict;

# Allow reference to collectl variables, but be CAREFUL as these should be treated as readonly
our ($miniFiller, $rate, $SEP, $datetime, $intSecs, $totSecs, $showColFlag);
our ($firstPass, $debug, $filename, $playback, $ioSizeFlag, $verboseFlag);
our ($OneKB, $OneMB, $OneGB, $TenGB);
our ($miniDateTime, $options, $FS, $Host, $XCFlag, $interval, $count);
our ($sameColsFlag, $subsys, $ReqDir);

require "$ReqDir/LustreSingleton.pm";
use lib "$ReqDir";
use LustreCommon;

# Global to this module
my $lustOpts = undef;
my $lustOptsOnly = undef;
my $lustre_singleton = new LustreSingleton();
my $lustre_version = $lustre_singleton->getVersion();
my $METRIC = {value => 0, last => 0};
my $printMsg = $debug & 16384;

my $lustreCltDirtyHitsTot = 0;
my $lustreCltDirtyMissTot = 0;
my $lustreCltReadTot = 0;
my $lustreCltReadKBTot = 0;
my $lustreCltWriteTot = 0;
my $lustreCltWriteKBTot = 0;
my $lustreCltOpenTot = 0;
my $lustreCltCloseTot = 0;
my $lustreCltSeekTot = 0;
my $lustreCltFsyncTot = 0;
my $lustreCltSetattrTot = 0;
my $lustreCltGetattrTot = 0;
my $lustreCltRAPendingTot = 0;
my $lustreCltRAHitsTot = 0;
my $lustreCltRAMissesTot = 0;
my $lustreCltRANotConTot = 0; # Summed over filesystems
my $lustreCltRAMisWinTot = 0;
my $lustreCltRAFailGrabTot = 0;
my $lustreCltRAFailLockTot = 0;
my $lustreCltRAReadDiscTot = 0;
my $lustreCltRAZeroLenTot = 0;
my $lustreCltRAZeroWinTot = 0;
my $lustreCltRA2EofTot = 0;
my $lustreCltRAHitMaxTot = 0;
my $lustreCltRAWrongTot = 0;

my $lustreCltReadKBTOT = 0;
my $lustreCltReadTOT = 0;
my $lustreCltWriteKBTOT = 0;
my $lustreCltWriteTOT = 0;
my $lustreCltRAHitsTOT = 0;
my $lustreCltRAMissesTOT = 0;

my $limLusKBS = 100;
my $limLusReints = 1000;

my $cltFlag = 0;
my $reportCltFlag = 0;
my @clientFSNames = ();
my $clientFSNamesStr = '';
my $numClientFS = 0; # Number of client filesystems
my $FSWidth = 0;
my $ostWidth = 0;
my @clientOstNames = ();
my $clientOstNamesStr = '';
my $numClientOsts = 0; # Number of active OSTs
my %clientFSData = (); # Filesystem performance data indexed by filesystem name
my %clientOstData = (); # OST performance data indexed by FSName.OST
my %clientOstRpcReadData = ();
my %clientOstRpcWriteData = ();
my @clientRpcReadTot = []; # indexed by buffer number
my @clientRpcWriteTot = []; # indexed by buffer number

# Global to count how many buckets there are for brw_stats
my @brwBuckets = [];
my $numBrwBuckets = scalar(@brwBuckets);

logmsg('I', "Lustre version $lustre_version") if $printMsg;

sub createClientFS {
  my $fs = {dir => '',
            commonName => '',
            dirty_pages_hits => {%{$METRIC}},
            dirty_pages_misses => {%{$METRIC}},
            read => {%{$METRIC}},
            readKB => {%{$METRIC}},
            write => {%{$METRIC}},
            writeKB => {%{$METRIC}},
            open => {%{$METRIC}},
            close => {%{$METRIC}},
            seek => {%{$METRIC}},
            fsync => {%{$METRIC}},
            setattr => {%{$METRIC}},
            getattr => {%{$METRIC}},
            ra_pending => {%{$METRIC}},
            ra_hits => {%{$METRIC}},
            ra_misses => {%{$METRIC}},
            ra_notcon => {%{$METRIC}},
            ra_miswin => {%{$METRIC}},
            ra_failgrab => {%{$METRIC}},
            ra_faillock => {%{$METRIC}},
            ra_readdisc => {%{$METRIC}},
            ra_zerowin => {%{$METRIC}},
            ra_zerolen => {%{$METRIC}},
            ra_2EofMax => {%{$METRIC}},
            ra_hitmax => {%{$METRIC}},
            ra_wrong => {%{$METRIC}}};
  return $fs;
}

sub createClientOst {
  my $clientOst = {fsName => '',
                   ostName => '',
                   dir => '',
                   lun_read => {%{$METRIC}},
                   lun_readKB => {%{$METRIC}},
                   lun_write => {%{$METRIC}},
                   lun_writeKB => {%{$METRIC}}};
  return $clientOst;
}

sub lustreCheckClient {
  my @saveClientFSNames = @clientFSNames;
  my $saveClientFSNamesStr = $clientFSNamesStr;
  
  # Get Filesystem Names
  
  $FSWidth = 0;
  @clientFSNames = ();
  $numClientFS = 0;

  my %clientFSNamesHash = ();
  my @lustreFS = glob("/proc/fs/lustre/llite/*");
  foreach my $dir (@lustreFS) {
    # in newer versions of lustre, the fs name was dropped from uuid, 
    # so look here instead which does exist in earlier versions too, 
    # but we didn't look there sooner because uuid is still used in 
    # other cases and I wanted to be consistent.
    my $commonName = cat("$dir/lov/common_name");
    chomp $commonName;
    my $fsName = (split(/-clilov-/, $commonName))[0];
    
    # we use the dirname for finding 'stats' and fsname for printing.
    # we may need the common name to make osts back to filesystems
    my $dirname = basename($dir);
    $clientFSNamesHash{$fsName} = $fsName;
    push(@clientFSNames, $fsName);

    logmsg(
      'I', 
      "Found filesystem $fsName, dir = $dirname, commonName = $commonName")
      if $printMsg;
    $numClientFS++;
    
    $clientFSData{$fsName} = createClientFS()
      if !exists($clientFSData{$fsName});
    $clientFSData{$fsName}{dir} = $dirname;
    $clientFSData{$fsName}{commonName} = $commonName;
    
    $FSWidth = length($fsName) if $FSWidth < length($fsName);
    $cltFlag = $reportCltFlag = 1;
  }
  $clientFSNamesStr = join(' ', @clientFSNames);
  $FSWidth++;

  my $changed = $clientFSNamesStr ne $saveClientFSNamesStr;
  # Remove any nonexistent filesystems
  if ($changed) {
    foreach my $fsName (@saveClientFSNames) {
      delete $clientFSData{$fsName} if !exists($clientFSNamesHash{$fsName});
    }
    
    my $comment = ($filename eq '') ? '#' : '';
    my $text = "Lustre CLT FSs Changed -- Old: $saveClientFSNamesStr  New: $clientFSNamesStr";
    logmsg('W', "${comment}$text") if !$firstPass;
    print "$text\n" if $firstPass && $printMsg;
  }
  
  # Only For '--lustopts  B/O' Get OST Names

  if ($cltFlag && $lustOpts =~ /[BO]/) {
    # we first need to get a list of all the OST uuids for 
    # all the filesystems, noting the 1 passed to cat() 
    # tells it to read until EOF
    my %clientOstMappings;
    foreach my $fsName (@clientFSNames) {
      my $obds = 
        cat("/proc/fs/lustre/lov/$clientFSData{$fsName}{commonName}/target_obd",
            1);

      logmsg('I', "Processing OSTS: fsName = $fsName, commonName = $clientFSData{$fsName}{commonName}, obds = $obds") if $printMsg;

      foreach my $obd (split(/\n/, $obds)) {
        my ($uuid, $state) = (split(/\s+/, $obd))[1, 2];
        logmsg('I', "obd = $obd, uuid = $uuid, state = $state") if $printMsg;
        next if $state ne 'ACTIVE';
        $clientOstMappings{$uuid} = $fsName;
      }
    }
    
    my @saveClientOstNames = @clientOstNames;
    my $saveClientOstNamesStr = $clientOstNamesStr;

    @clientOstNames = ();
    $numClientOsts = 0;

    my %clientOstNamesHash = ();
    my @lustreDirs = glob("/proc/fs/lustre/osc/*");
    foreach my $dir (@lustreDirs) {
      # Since we're looking for OST subdirectories, ignore anything not 
      # a directory which for now is limted to 'num_refs', but who 
      # knows what the future will hold.  As for the 'MNT' test, I think 
      # that only applied to older versions of lustre, certainlu tp HP-SFS.
      next if !-d $dir; # currently only the 'num_refs' file

      # Looks like if you're on a 1.6.4.3 system (and perhaps earlier) 
      # that is both a client as well as an MDS, you'll see MDS 
      # specific directories with names like - lustre-OST0000-osc, 
      # whereas lustre-OST0000-osc-000001012e950400 is the
      # client directory we want, so...
      next if $dir =~ /\-osc$/;

      logmsg('I', "Processing osc dir $dir") if $printMsg;
      
      # if ost closed (this happens when new filesystems get created), 
      # ignore it. note that newer versions of lustre added a sstate 
      # and sets it to DEACTIVATED
      my ($uuid, $state, $sstate) = split(/\s+/, cat("$dir/ost_server_uuid"));

      logmsg('I', "uuid = $uuid, state = $state, sstate = $sstate")
        if $printMsg;

      next if $state =~ /CLOSED|DISCONN/ || $sstate =~ /DEACT/;

      # uuids look something like 'xxx-ost_UUID' and you can actully 
      # have a - or _ following the xxx so drop the beginning/end 
      # this way in case an embedded _ in ost name itself.
      my $ostName = $uuid;
      $ostName =~ s/.*[-](OST.*)_UUID/$1/;
      my $fsName = $clientOstMappings{$uuid};

      logmsg('I', "ostName = $ostName, fsName = $fsName") if $printMsg;
      
      $ostWidth = length($ostName) if $ostWidth < length($ostName);

      my $ostFullName = "$fsName.$ostName";
      $clientOstNamesHash{$ostFullName} = $ostFullName;
      push(@clientOstNames, $ostFullName);
      $numClientOsts++;

      if (!exists($clientOstData{$ostFullName})) {
        logmsg('I', "Adding client OST $ostFullName") if $printMsg;

        $clientOstData{$ostFullName} = createClientOst();
        for (my $j = 0; $j < $numBrwBuckets; $j++) {
          $clientOstRpcReadData{$ostFullName}[$j] = {%{$METRIC}};
          $clientOstRpcWriteData{$ostFullName}[$j] = {%{$METRIC}};
        }
      }

      $clientOstData{$ostFullName}{fsName} = $fsName;
      $clientOstData{$ostFullName}{ostName} = $ostName;
      $clientOstData{$ostFullName}{dir} = $dir;
    }
    $clientOstNamesStr = join(' ', @clientOstNames);

    # Remove any nonexistent OSTs
    if ($clientOstNamesStr ne $saveClientOstNamesStr) {
      foreach my $ostFullName (@saveClientOstNames) {
        if (!exists($clientOstNamesHash{$ostFullName})) {
          logmsg('I', "Deleting client OST $ostFullName") if $printMsg;

          delete $clientOstData{$ostFullName};
          delete $clientOstRpcReadData{$ostFullName};
          delete $clientOstRpcWriteData{$ostFullName};
        }
      }
    }
    
    $ostWidth = 3 if $ostWidth < 3;

    my $clientOstNamesChanged = $clientOstNamesStr ne $saveClientOstNamesStr;
    $changed = $changed || $clientOstNamesChanged;
    # Change info is important even when not logging except during 
    # initialization
    if ($clientOstNamesChanged) {
      my $comment = ($filename eq '') ? '#' : '';
      my $text = "Lustre CLT OSTs Changed -- Old: $saveClientOstNamesStr  New: $clientOstNamesStr";
      logmsg('W', "${comment}$text") if !$firstPass;
      print "$text\n" if $firstPass && $printMsg;
    }
  }
  
  return $changed ? 1 : 0;
}

sub getClientLliteStats {
  my ($fsName, $fsdir) = @_;
  my $tag = "LLITE:$fsName";

  my $proc = "/proc/fs/lustre/llite/$fsdir/stats";
  return(0) if (!open PROC, "<$proc");

  while (my $line = <PROC>) {
    if (($line =~ /^dirty/) || 
        ($line =~ /^read/) || 
        ($line =~ /^write/) || 
        ($line =~ /^open/) || 
        ($line =~ /^close/) || 
        ($line =~ /^seek/) || 
        ($line =~ /^fsync/) || 
        ($line =~ /^getattr/) || 
        ($line =~ /^getxattr/) || 
        ($line =~ /^setattr/) || 
        ($line =~ /^setxattr/) || 
        ($line =~ /^statfs/)) { 
      record(2, "$tag $line"); 
    }
  }
  close PROC;
  return(1);
}

sub getClientReadAheadStats {
  my ($fsName, $fsdir) = @_;
  my $tag = "LLITE_RA:$fsName";

  my $proc = "/proc/fs/lustre/llite/$fsdir/read_ahead_stats";
  return(0) if (!open PROC, "<$proc");

  while (my $line = <PROC>) {
    record(2, "$tag $line"); 
  }
  close PROC;
  return(1);
}

sub getClientRpcStats {
  my ($ostName, $ostdir) = @_;
  my $tag = "LLITE_RPC:$ostName";
  
  my $proc = "$ostdir/rpc_stats";
  return(0) if (!open PROC, "<$proc");

  # Skip to beginning of rpcdata
  while (my $line = <PROC>) {
    last if (index($line, "pages per rpc") == 0);
  }

  my $index = 0;
  while (my $line = <PROC>) {
    last    if $line=~/^\s+$/;
    record(2, "$tag:$index $line");
    $index++;
  }
  close PROC;
  return(1);
}

sub getClientOstStats {
  my ($ostName, $ostdir) = @_;
  my $tag = "LLDET:$ostName";

  my $proc = "$ostdir/stats";
  return(0) if (!open PROC, "<$proc");

  while (my $line = <PROC>) {
    if (($line=~/^read_bytes/) ||
        ($line=~/^write_bytes/)) {
      record(2, "$tag $line");
    }
  }
  close PROC;
  return(1);
}

sub lustreGetClientStats {
  foreach my $fsName (@clientFSNames) {
    # For vanilla -sl we only need read/write info, but lets grab metadata file 
    # we're at it.  In the case of --lustopts R, we also want readahead stats
    getClientLliteStats($fsName, $clientFSData{$fsName}{dir});
    getClientReadAheadStats($fsName, $clientFSData{$fsName}{dir}) 
      if $lustOpts =~ /R/;
  }
  
  # RPC stats are optional for both clients and servers
  if ($lustOpts =~ /B/) {
    foreach my $ostName (@clientOstNames) {
      getClientRpcStats($ostName, $clientOstData{$ostName}{dir});
    }
  }
  
  # Client OST detail data
  if ($lustOpts =~ /O/) {
    foreach my $ostName (@clientOstNames) {
      getClientOstStats($ostName, $clientOstData{$ostName}{dir});
    }
  }
  return(1);
}

sub lustreClientInit {
  my $impOptsref = shift;
  my $impKeyref = shift;

  error("You must remove the -sl or -sL option to use this plugin")
    if ($subsys =~ /l/i);

  $lustOpts = ${$impOptsref};
  error('Valid lustre options are: s d B M O R') 
    if defined($lustOpts) && $lustOpts !~ /^[sdBMOR]*$/;

  $lustOpts = 's' if !defined($lustOpts);
  ${impOptsref} = $lustOpts;

  error("Lustre does not appear to be installed on this host") 
    if (!defined $lustre_version);

  error("Lustre versions earlier than 1.8.0 are not currently supported")
    if ($lustre_version lt '1.8.0');

  print "lustreClientInit: options: $lustOpts\n" if $printMsg;

  ${$impKeyref} = 'LLITE|LLDET';

  error("You cannot mix Lustre option 'O' with 'M' or 'R'") 
    if $lustOpts =~ /O/ && $lustOpts =~ /[MR]/;
  
  error("You cannot mix Lustre option 'B' with 'M'") 
    if $lustOpts =~ /B/ && $lustOpts =~ /M/;
  
  error("You cannot mix Lustre option 'B' with 'R'") 
    if $lustOpts =~ /B/ && $lustOpts =~ /R/;

  @brwBuckets = (1,2,4,8,16,32,64,128,256);
  $numBrwBuckets = scalar(@brwBuckets);

  lustreCheckClient();

  error("Lustre option O only applies to client detail data") 
    if $lustOpts =~ /O/ && (!$cltFlag || $lustOpts !~ /d/);
  
  # Force if not already specified, but ONLY for details
  $lustOpts .= 'O' 
    if $cltFlag && $lustOpts =~ /d/ && $lustOpts =~ /B/ && $lustOpts !~ /O/;

  $verboseFlag = 1 if $lustOpts =~ /[BM]/;

  $lustOptsOnly = $lustOpts;
  $lustOptsOnly =~ s/[ds]//;

  return(1);
}

sub lustreClientUpdateHeader {
  my $lineref = shift;

  ${$lineref} .= 
    "# Lustre Client Data Collector: Version 1.0, Lustre version: $lustre_version\n";
}

sub lustreClientGetData {
  lustreGetClientStats() if ($numClientFS > 0);
}

sub lustreClientInitInterval {
  # Check to see if any services changed and if they did, we may need
  # a new logfile as well.
  newLog($filename, "", "", "", "", "") 
    if lustreCheckClient() && $filename ne '';

  $sameColsFlag = 0 if length($lustOptsOnly) > 1;

  $lustreCltDirtyHitsTot = 0;
  $lustreCltDirtyMissTot = 0;
  $lustreCltReadTot = 0;
  $lustreCltReadKBTot = 0;
  $lustreCltWriteTot = 0;
  $lustreCltWriteKBTot = 0;
  $lustreCltOpenTot = 0;
  $lustreCltCloseTot = 0;
  $lustreCltSeekTot = 0;
  $lustreCltFsyncTot = 0;
  $lustreCltSetattrTot = 0;
  $lustreCltGetattrTot = 0;
  $lustreCltRAPendingTot = 0;
  $lustreCltRAHitsTot = 0;
  $lustreCltRAMissesTot = 0;
  $lustreCltRANotConTot = 0;
  $lustreCltRAMisWinTot = 0;
  $lustreCltRAFailGrabTot = 0;
  $lustreCltRAFailLockTot = 0;
  $lustreCltRAReadDiscTot = 0;
  $lustreCltRAZeroLenTot = 0;
  $lustreCltRAZeroWinTot = 0;
  $lustreCltRA2EofTot = 0;
  $lustreCltRAHitMaxTot = 0;
  $lustreCltRAWrongTot = 0;
  for (my $i = 0; $i <= $numBrwBuckets; $i++) {
    $clientRpcReadTot[$i] = 0;
    $clientRpcWriteTot[$i] = 0;
  }
}

sub lustreClientAnalyze {
  my $type = shift;
  my $dataref = shift;
  my $data = ${$dataref};

  logmsg('I', "lustreClientAnalyze: type: $type, data: $data") if $printMsg;

  if ($type =~ /LLITE:(.+)/) {
    my $fsName = $1;
    my ($metric, $ops, $value) = (split(/\s+/, $data))[0, 1, 6];

    my $attrId = undef;
    my $tot = undef;
    if ($metric =~ /dirty_pages_hits/) {
      my $attrId = 'dirty_pages_hits';
      $tot = \$lustreCltDirtyHitsTot;
    } elsif ($metric =~ /dirty_pages_misses/) {
      $attrId = 'dirty_pages_misses';
      $tot = \$lustreCltDirtyMissTot;
    } elsif ($metric =~ /read_bytes/) {
      $attrId = 'read';
      $tot = \$lustreCltReadTot;

      $value = 0 if !defined($value);
      my $readKB = $clientFSData{$fsName}{readKB};
      $readKB->{value} = LustreCommon::delta($value, $readKB->{last}) / $OneKB;
      $readKB->{last} = $value;
      $lustreCltReadKBTot += $readKB->{value};
    } elsif ($metric =~ /write_bytes/) {
      $attrId = 'write';
      $tot = \$lustreCltWriteTot;

      $value = 0 if !defined($value);
      my $writeKB = $clientFSData{$fsName}{writeKB};
      $writeKB->{value} = LustreCommon::delta($value, $writeKB->{last}) / $OneKB;
      $writeKB->{last} = $value;
      $lustreCltWriteKBTot += $writeKB->{value};
    } elsif ($metric =~ /open/) { 
      $attrId = 'open';
      $tot = \$lustreCltOpenTot;
    } elsif ($metric =~ /close/) {
      $attrId = 'close';
      $tot = \$lustreCltCloseTot;
    } elsif ($metric =~ /seek/) {
      $attrId = 'seek';
      $tot = \$lustreCltSeekTot;
    } elsif ($metric =~ /fsync/) {
      $attrId = 'fsync';
      $tot = \$lustreCltFsyncTot;
    } elsif ($metric =~ /setattr/) {
      $attrId = 'setattr';
      $tot = \$lustreCltSetattrTot;
    } elsif ($metric =~ /getattr/) {
      $attrId = 'getattr';
      $tot = \$lustreCltGetattrTot;
    }

    if (defined($attrId) && defined($ops)) {
      my $attr = $clientFSData{$fsName}{$attrId};
      my $value = LustreCommon::delta($ops, $attr->{last});
      $attr->{value} = $value;
      $attr->{last} = $ops;
      ${$tot} += $value if defined($tot);
    }
  } elsif ($type =~ /LLITE_RA:(.+)/) {
    my $fsName = $1;

    my $attrId = undef;
    my $tot = undef;
    my $ops = undef;
    if ($data =~ /^pending.* (\d+)/) {
      # This is NOT a counter but a meter
      my $pending = $clientFSData{$fsName}{ra_pending};
      $pending->{value} = $1;
      $lustreCltRAPendingTot += $pending->{value};
    } elsif ($data =~ /^hits.* (\d+)/) {
      $ops = $1;
      $attrId = 'ra_hits';
      $tot = \$lustreCltRAHitsTot;
    } elsif ($data =~ /^misses.* (\d+)/) {
      $ops = $1;
      $attrId = 'ra_misses';
      $tot = \$lustreCltRAMissesTot;
    } elsif ($data =~ /^readpage.* (\d+)/) {
      $ops = $1;
      $attrId = 'ra_notcon';
      $tot = \$lustreCltRANotConTot;
    } elsif ($data =~ /^miss inside.* (\d+)/) {
      $ops = $1;
      $attrId = 'ra_miswin';
      $tot = \$lustreCltRAMisWinTot;
    } elsif ($data =~ /^failed grab.* (\d+)/) {
      $ops = $1;
      $attrId = 'ra_failgrab';
      $tot = \$lustreCltRAFailGrabTot;
    } elsif ($data =~ /^failed lock.* (\d+)/) {
      $ops = $1;
      $attrId = 'ra_faillock';
      $tot = \$lustreCltRAFailLockTot;
    } elsif ($data =~ /^read but.* (\d+)/) {
      $ops = $1;
      $attrId = 'ra_readdisc';
      $tot = \$lustreCltRAReadDiscTot;
    } elsif ($data =~ /^zero length.* (\d+)/) {
      $ops = $1;
      $attrId = 'ra_zerolen';
      $tot = \$lustreCltRAZeroLenTot;
    } elsif ($data =~ /^zero size.* (\d+)/) {
      $ops = $1;
      $attrId = 'ra_zerowin';
      $tot = \$lustreCltRAZeroWinTot;
    } elsif ($data =~ /^read-ahead.* (\d+)/) {
      $ops = $1;
      $attrId = 'ra_2EofMax';
      $tot = \$lustreCltRA2EofTot;
    } elsif ($data =~ /^hit max.* (\d+)/) {
      $ops = $1;
      $attrId = 'ra_hitmax';
      $tot = \$lustreCltRAHitMaxTot;
    } elsif ($data =~ /^wrong.* (\d+)/) {
      $ops = $1;
      $attrId = 'ra_wrong';
      $tot = \$lustreCltRAWrongTot;
    }

    if (defined($attrId) && defined($ops)) {
      my $attr = $clientFSData{$fsName}{$attrId};
      my $value = LustreCommon::delta($ops, $attr->{last});
      $attr->{value} = $value;
      $attr->{last} = $ops;
      ${$tot} += $value if defined($tot);
    }
  } elsif ($type =~ /LLITE_RPC:(.+):(\d+)/) {
    my $ostName = $1;
    my $bufNum = $2;

    my ($reads, $writes) = (split(/\s+/, $data))[1, 5];

    my $attr = $clientOstRpcReadData{$ostName}[$bufNum];
    $attr->{value} = LustreCommon::delta($reads, $attr->{last});
    $attr->{last} = $reads;
    $clientRpcReadTot[$bufNum] += $attr->{value};

    $attr = $clientOstRpcWriteData{$ostName}[$bufNum];
    $attr->{value} = LustreCommon::delta($writes, $attr->{last});
    $attr->{last} = $writes;
    $clientRpcWriteTot[$bufNum] += $attr->{value};
  } elsif ($type =~ /LLDET:(.*)/) {
    my $ostName = $1;
    my ($metric, $ops, $value) = (split(/\s+/, $data))[0, 1, 6];
    
    if ($metric =~ /^read_bytes|ost_r/) {
      my $attr = $clientOstData{$ostName}{lun_read};
      $attr->{value} = LustreCommon::delta($ops, $attr->{last});
      $attr->{last} = $ops;
      if (defined($value)) { # not always defined
        $attr = $clientOstData{$ostName}{lun_readKB};
        $attr->{value} = LustreCommon::delta($value, $attr->{last}) / $OneKB;
        $attr->{last} = $value;
      }
    } elsif ($metric =~ /^write_bytes|ost_w/) {
      my $attr = $clientOstData{$ostName}{lun_write};
      $attr->{value} = LustreCommon::delta($ops, $attr->{last});
      $attr->{last} = $ops;
      if (defined($value)) { # not always defined
        $attr = $clientOstData{$ostName}{lun_writeKB};
        $attr->{value} = LustreCommon::delta($value, $attr->{last}) / $OneKB;
        $attr->{last} = $value;
      }
    }
  }
}

# This and the 'print' routines should be self explanitory as they pretty much simply
# return a string in the appropriate format for collectl to dispose of.
sub lustreClientPrintBrief {
  my $type = shift;
  my $lineref = shift;
  
  if ($type == 1) { # header line 1
    if ($lustOpts =~ /s/ && $reportCltFlag) {
      ${$lineref} .= "<--------Lustre Client-------->"
        if !$ioSizeFlag && $lustOpts !~ /R/;
      ${$lineref} .= "<---------------Lustre Client--------------->"
        if !$ioSizeFlag && $lustOpts =~ /R/;
      ${$lineref} .= "<-------------Lustre Client------------->"
        if $ioSizeFlag && $lustOpts !~ /R/;
      ${$lineref} .= "<--------------------Lustre Client-------------------->"
        if  $ioSizeFlag && $lustOpts =~ /R/;
    }
  } elsif ($type == 2) { # header line 2
    if ($lustOpts =~ /s/ && $reportCltFlag) {
      ${$lineref} .= " KBRead  Reads  KBWrite Writes" if !$ioSizeFlag;
      ${$lineref} .= " KBRead  Reads Size  KBWrite Writes Size" 
        if $ioSizeFlag;
      ${$lineref} .= "   Hits Misses" if $lustOpts =~ /R/;
    }
  } elsif ($type == 3) { # data
    if ($lustOpts =~ /s/ && $reportCltFlag) {
      if (!$ioSizeFlag) {
        ${$lineref} .= 
          sprintf("%7s %6s  %7s %6s", 
                  cvt($lustreCltReadKBTot / $intSecs, 7, 0, 1), 
                  cvt($lustreCltReadTot / $intSecs),
                  cvt($lustreCltWriteKBTot / $intSecs, 7, 0, 1),
                  cvt($lustreCltWriteTot / $intSecs, 6));
      } else {
        ${$lineref} .= 
          sprintf("%7s %6s %4s  %7s %6s %4s",
                  cvt($lustreCltReadKBTot / $intSecs, 7, 0, 1),
                  cvt($lustreCltReadTot / $intSecs),
                  $lustreCltReadTot ? cvt($lustreCltReadKBTot / $lustreCltReadTot, 4, 0, 1) : 0,
                  cvt($lustreCltWriteKBTot / $intSecs, 7, 0, 1),
                  cvt($lustreCltWriteTot / $intSecs, 6),
                  $lustreCltWriteTot ? cvt($lustreCltWriteKBTot / $lustreCltWriteTot, 4, 0, 1) : 0);
      }
    
      # Add in cache hits/misses if --lustopts R
      ${$lineref} .= 
        sprintf(" %6d %6d",
                $lustreCltRAHitsTot,
                $lustreCltRAMissesTot) if $lustOpts =~ /R/;
    }
  } elsif ($type == 4) { # reset 'total' counters
    $lustreCltReadKBTOT = 0;
    $lustreCltReadTOT = 0;
    $lustreCltWriteKBTOT = 0;
    $lustreCltWriteTOT = 0;
    $lustreCltRAHitsTOT = 0;
    $lustreCltRAMissesTOT = 0;
  } elsif ($type == 5) { # increment 'total' counters
    if ($numClientFS) {
      $lustreCltReadTOT += $lustreCltReadTot;
      $lustreCltReadKBTOT += $lustreCltReadKBTot;
      $lustreCltWriteTOT += $lustreCltWriteTot;
      $lustreCltWriteKBTOT += $lustreCltWriteKBTot;

      $lustreCltRAHitsTOT += $lustreCltRAHitsTot;
      $lustreCltRAMissesTOT += $lustreCltRAMissesTot;
    }
  } elsif ($type == 6) { # print 'total' counters
    # Since this never goes over a socket we can just do a simple print.
    
    if ($lustOpts =~ /s/ && $reportCltFlag) {
      if (!$ioSizeFlag) {
        printf "%7s %6s  %7s %6s", 
        cvt($lustreCltReadKBTOT / $totSecs, 7, 0, 1), 
        cvt($lustreCltReadTOT / $totSecs, 6),
        cvt($lustreCltWriteKBTOT / $totSecs, 7, 0, 1),
        cvt($lustreCltWriteTOT / $totSecs, 6);
      } else {
        printf  "%7s %6s %4s  %7s %6s %4s",
        cvt($lustreCltReadKBTOT / $totSecs, 7, 0, 1), 
        cvt($lustreCltReadTOT / $totSecs, 6),
        $lustreCltReadTOT ? cvt($lustreCltReadKBTOT / $lustreCltReadTOT,
                                4, 0, 1) : 0,
        cvt($lustreCltWriteKBTOT / $totSecs, 7, 0, 1),
        cvt($lustreCltWriteTOT / $totSecs, 6),
        $lustreCltWriteTOT ? cvt($lustreCltWriteKBTOT / $lustreCltWriteTOT,
                                 4, 0, 1) : 0;
      }
      printf " %6s %6s", 
      cvt($lustreCltRAHitsTOT / $totSecs, 6),
      cvt($lustreCltRAMissesTOT / $totSecs, 6) if $lustOpts =~ /R/;
    }
  }
}

sub lustreClientPrintVerbose {
  my $printHeader = shift;
  my $homeFlag = shift;
  my $lineref = shift;

  # Note that last line of verbose data (if any) still sitting in $$lineref
  my $line = ${$lineref} = '';
  
  # NOTE - there are a number of different types of formats here and 
  # we're always going to include reads/writes with all of them!
  if ($lustOpts =~ /s/ && $reportCltFlag) {
    # If time for common header, do it...
    if ($printHeader) {
      my $line = '';
      $line .= "\n" if !$homeFlag;
      $line .= "# LUSTRE CLIENT SUMMARY ($rate)";
      $line .= ":" if $lustOpts =~ /[BMR]/;
      $line .= " RPC-BUFFERS (pages)" if $lustOpts =~ /B/;
      $line .= " METADATA" if $lustOpts =~ /M/;
      $line .= " READAHEAD" if $lustOpts =~ /R/;
      $line .= "\n";
      ${$lineref} .= $line;
    }
    
    # If exception processing must be above minimum
    if ($options !~ /x/ || 
        $lustreCltReadKBTot / $intSecs >= $limLusKBS ||
        $lustreCltWriteKBTot / $intSecs >= $limLusKBS) {
      if ($lustOpts !~ /[BMR]/) {
        ${$lineref} .= "#$miniDateTime  KBRead  Reads SizeKB   KBWrite Writes SizeKB\n" if $printHeader;
        exit if $showColFlag;
        
        my $line = sprintf("$datetime  %7d %6d %6d   %7d %6d %6d\n",
                           $lustreCltReadKBTot / $intSecs,  
                           $lustreCltReadTot / $intSecs,
                           $lustreCltReadTot ? 
                           int($lustreCltReadKBTot / $lustreCltReadTot) : 0,
                           $lustreCltWriteKBTot / $intSecs,
                           $lustreCltWriteTot / $intSecs,
                           $lustreCltWriteTot ? 
                           int($lustreCltWriteKBTot / $lustreCltWriteTot) : 0);
        ${$lineref} .= $line;
      }
      
      if ($lustOpts =~ /B/) {
        if ($printHeader) {
          my $temp = '';
          foreach my $i (@brwBuckets) {
            $temp .= sprintf(" %3dP", $i); 
          }
          ${$lineref} .= "#${miniDateTime}RdK  Rds$temp WrtK Wrts$temp\n";
          exit if $showColFlag;
        }
        
        my $line = "$datetime";
        $line .= sprintf("%4s %4s", 
                         cvt($lustreCltReadKBTot / $intSecs, 4, 0, 1),
                         cvt($lustreCltReadTot / $intSecs));
        for (my $i = 0; $i < $numBrwBuckets; $i++) {
          $line .= sprintf(" %4s", cvt($clientRpcReadTot[$i] / $intSecs));
        }
        
        $line .= sprintf(" %4s %4s",
                         cvt($lustreCltWriteKBTot / $intSecs, 4, 0, 1),
                         cvt($lustreCltWriteTot / $intSecs));
        for (my $i = 0; $i < $numBrwBuckets; $i++) {
          $line .= sprintf(" %4s", cvt($clientRpcWriteTot[$i] / $intSecs));
        }
        $line .= "\n";
        ${$lineref} .= $line;
      }
      
      if ($lustOpts =~ /M/) {
        ${$lineref} .= "#$miniDateTime  KBRead  Reads  KBWrite Writes  Open Close GAttr SAttr  Seek Fsync DrtHit DrtMis\n" if $printHeader;
        exit if $showColFlag;
        
        my $line = sprintf("$datetime  %7d %6d  %7d %6d %5d %5d %5d %5d %5d %5d %6d %6d\n",
                           $lustreCltReadKBTot / $intSecs, 
                           $lustreCltReadTot / $intSecs,   
                           $lustreCltWriteKBTot / $intSecs, 
                           $lustreCltWriteTot / $intSecs,   
                           $lustreCltOpenTot / $intSecs, 
                           $lustreCltCloseTot / $intSecs, 
                           $lustreCltGetattrTot / $intSecs,
                           $lustreCltSetattrTot / $intSecs, 
                           $lustreCltSeekTot / $intSecs, 
                           $lustreCltFsyncTot / $intSecs,  
                           $lustreCltDirtyHitsTot / $intSecs,
                           $lustreCltDirtyMissTot / $intSecs);
        ${$lineref} .= $line;
      }
      
      if ($lustOpts =~ /R/) {
        ${$lineref} .= "#$miniDateTime  KBRead  Reads  KBWrite Writes  Pend  Hits Misses NotCon MisWin FalGrb LckFal  Discrd ZFile ZerWin RA2Eof HitMax  Wrong\n" if $printHeader;
        exit if $showColFlag;
        
        my $line = sprintf("$datetime  %7d %6d  %7d %6d %5d %5d %6d %6d %6d %6d %6d %6d %6d %6d %6d %6d %6d\n",
                           $lustreCltReadKBTot / $intSecs,
                           $lustreCltReadTot / $intSecs,   
                           $lustreCltWriteKBTot / $intSecs,  
                           $lustreCltWriteTot / $intSecs,   
                           $lustreCltRAPendingTot / $intSecs,
                           $lustreCltRAHitsTot / $intSecs,
                           $lustreCltRAMissesTot / $intSecs,
                           $lustreCltRANotConTot / $intSecs,
                           $lustreCltRAMisWinTot / $intSecs,
                           $lustreCltRAFailGrabTot / $intSecs,
                           $lustreCltRAFailLockTot / $intSecs,
                           $lustreCltRAReadDiscTot / $intSecs, 
                           $lustreCltRAZeroLenTot / $intSecs,
                           $lustreCltRAZeroWinTot / $intSecs,  
                           $lustreCltRA2EofTot / $intSecs,
                           $lustreCltRAHitMaxTot / $intSecs,
                           $lustreCltRAWrongTot / $intSecs);
        ${$lineref} .= $line;
      }
    }
  }
  
  # NOTE -- there are 2 levels of details, both with and without --lustopts O
  if ($lustOpts =~ /d/ && $reportCltFlag) {
    my $fill1 = '';
    my $temp = '';
    if ($printHeader) {
      # we need to build filesystem header, and when no date/time make it even 1
      # char less.
      $temp = "Filsys" . ' 'x$FSWidth;
      $temp = substr($temp, 0, $FSWidth);
      $temp = substr($temp, 0, $FSWidth - 2) . ' ' if $miniFiller eq '';
      
      # When doing dates/time, we also need to shift first field over 1 
      # to the left;
      $fill1 = '';
      if ($miniFiller ne '') {
        $fill1 = substr($miniDateTime, 0, length($miniFiller) - 1);
      }
      
      my $line = '';
      $line .= "\n" if !$homeFlag;
      $line .= "# LUSTRE CLIENT DETAIL ($rate)";
      $line .= ":" if $lustOpts =~ /[BMR]/;
      $line .= " RPC-BUFFERS (pages)" if $lustOpts =~ /B/;
      $line .= " METADATA" if $lustOpts =~ /M/;
      $line .= " READAHEAD" if $lustOpts =~ /R/;
      $line .= "\n";
      ${$lineref} .= $line;
    }
    
    if ($lustOpts =~ /O/) {
      # Never for M or R
      if ($lustOpts !~ /B/) {
        my $fill2 = ' 'x($ostWidth - 3);
        ${$lineref} .= "#$fill1$temp Ost$fill2  KBRead  Reads SizeKB  KBWrite Writes SizeKB\n" if $printHeader;
        exit if $showColFlag;
        
        foreach my $ostName (@clientOstNames) {
          my $ost = $clientOstData{$ostName};
          my $line = 
            sprintf("$datetime%-${FSWidth}s %-${ostWidth}s %7d %6d %6d  %7d %6d %6d\n",
                    $ost->{fsName},
                    $ost->{ostName},
                    $ost->{lun_readKB}{value} / $intSecs,
                    $ost->{lun_read}{value} /$intSecs,
                    $ost->{lun_read}{value} ?
                    $ost->{lun_readKB}{value} / $ost->{lun_read}{value} : 0,
                    $ost->{lun_writeKB}{value} / $intSecs,
                    $ost->{lun_write}{value} /$intSecs,
                    $ost->{lun_write}{value} ?
                    $ost->{lun_writeKB}{value} / $ost->{lun_write}{value} : 0);
          ${$lineref} .= $line;
        }
      }
      
      if ($lustOpts =~ /B/) {
        my $fill2 = ' 'x($ostWidth - 3);
        if ($printHeader) {
          my $temp2 = ' 'x(length("$fill1$temp Ost$fill2 "));
          my $temp3 = '';
          foreach my $i (@brwBuckets) {
            $temp3 .= sprintf(" %3dP", $i); 
          }
          ${$lineref} .= "#$fill1$temp Ost$fill2 RdK  Rds$temp3 WrtK Wrts$temp3\n";
        }
        for my $ostName (@clientOstNames) {
          my $ost = $clientOstData{$ostName};
          my $line = sprintf("$datetime%-${FSWidth}s %-${ostWidth}s",
                             $ost->{fsName},
                             $ost->{ostName});
          $line .= sprintf("%4s %4s", 
                           cvt($ost->{lun_readKB}{value} / $intSecs, 4,0,1),
                           cvt($ost->{lun_read}{value} / $intSecs));
          
          for (my $i = 0; $i < $numBrwBuckets; $i++) {
            $line .= sprintf(
              " %4s", 
              cvt($clientOstRpcReadData{$ostName}[$i]{value} / $intSecs));
          }
          
          $line .= sprintf(" %4s %4s",
                           cvt($ost->{lun_writeKB}{value} / $intSecs, 4,0,1),
                           cvt($ost->{lun_write}{value} / $intSecs));
          for (my $i = 0; $i < $numBrwBuckets; $i++) {
            $line .= sprintf(
              " %4s",
              cvt($clientOstRpcWriteData{$ostName}[$i]{value} / $intSecs));
          }
          $line .= "\n";
          ${$lineref} .= $line;
        }
      }
    } else {
      my $commonLine = 
        "#$fill1$temp  KBRead  Reads SizeKB  KBWrite Writes SizeKB";
      if ($lustOpts !~ /[MR]/) {
        ${$lineref} .= "$commonLine\n" if $printHeader;
        exit if $showColFlag;
        
        for my $fsName (@clientFSNames) {
          my $fs = $clientFSData{$fsName};
          my $line = 
            sprintf("$datetime%-${FSWidth}s %7d %6d %6d  %7d %6d %6d\n",
                    $fsName,
                    $fs->{readKB}{value} / $intSecs, 
                    $fs->{read}{value} / $intSecs,
                    $fs->{read}{value} ? 
                    $fs->{readKB}{value} / $fs->{read}{value} : 0,
                    $fs->{writeKB}{value} / $intSecs, 
                    $fs->{write}{value} / $intSecs,
                    $fs->{write}{value} ? 
                    $fs->{writeKB}{value} / $fs->{write}{value} : 0);
          ${$lineref} .= $line;
        }
      }
      
      if ($lustOpts =~ /M/) {
        ${$lineref} .= 
          "$commonLine  Open Close GAttr SAttr  Seek Fsync DrtHit DrtMis\n" 
          if $printHeader;
        exit if $showColFlag;
        
        {
          foreach my $fsName (@clientFSNames) {
            my $fs = $clientFSData{$fsName};
            my $line = 
              sprintf("$datetime%-${FSWidth}s %7d %6d %6d  %7d %6d %6d %5d %5d %5d %5d %5d %5d %6d %6d\n",
                      $fsName,
                      $fs->{readKB}{value} / $intSecs,
                      $fs->{read}{value} / $intSecs,
                      $fs->{read}{value} ?
                      $fs->{readKB}{value} / $fs->{read}{value} : 0,
                      $fs->{writeKB}{value} / $intSecs,
                      $fs->{write}{value} / $intSecs,
                      $fs->{write}{value} ?
                      $fs->{writeKB}{value} / $fs->{write}{value} : 0,
                      $fs->{open}{value} / $intSecs,
                      $fs->{close}{value} / $intSecs,
                      $fs->{getattr}{value} / $intSecs,
                      $fs->{setattr}{value} / $intSecs,
                      $fs->{seek}{value} / $intSecs,
                      $fs->{fsync}{value} / $intSecs,
                      $fs->{dirty_pages_hits}{value} / $intSecs,
                      $fs->{dirty_pages_misses}{value} / $intSecs);
            ${$lineref} .= $line;
          }
        }
      }
      
      if ($lustOpts =~ /R/) {
        ${$lineref} .= "$commonLine  Pend  Hits Misses NotCon MisWin FalGrb LckFal  Discrd ZFile ZerWin RA2Eof HitMax  Wrong\n" if $printHeader;
        exit if $showColFlag;
        
        {
          foreach my $fsName (@clientFSNames) {
            my $fs = $clientFSData{$fsName};
            my $line = 
              sprintf("$datetime%-${FSWidth}s %7d %6d %6d  %7d %6d %6d %5d %5d %6d %6d %6d %6d %6d %6d %6d %6d %6d %6d %6d\n",
                      $fsName,
                      $lustreCltReadKBTot / $intSecs,
                      $lustreCltReadTot / $intSecs, 
                      $fs->{read}{value} ? 
                      $fs->{readKB}{value} / $fs->{read}{value} : 0,
                      $lustreCltWriteKBTot / $intSecs, 
                      $lustreCltWriteTot / $intSecs, 
                      $fs->{write}{value} ? 
                      $fs->{writeKB}{value} / $fs->{write}{value} : 0,
                      $lustreCltRAPendingTot / $intSecs,
                      $lustreCltRAHitsTot / $intSecs,
                      $lustreCltRAMissesTot / $intSecs,
                      $lustreCltRANotConTot / $intSecs,
                      $lustreCltRAMisWinTot / $intSecs, 
                      $lustreCltRAFailGrabTot / $intSecs,
                      $lustreCltRAFailLockTot / $intSecs,
                      $lustreCltRAReadDiscTot / $intSecs,
                      $lustreCltRAZeroLenTot / $intSecs,
                      $lustreCltRAZeroWinTot / $intSecs,
                      $lustreCltRA2EofTot / $intSecs,
                      $lustreCltRAHitMaxTot / $intSecs,
                      $lustreCltRAWrongTot / $intSecs);
            ${$lineref} .= $line;
          }
        }
      }
    }
  }
}

# Just be sure to use $SEP in the right places.  A simple trick to make sure you've done it
# correctly is to generste a small plot file and load it into a speadsheet, making sure each
# column of data has a header and that they aling 1:1.
sub lustreClientPrintPlot {
  my $type = shift;
  my $ref1 = shift;

  #    H e a d e r s

  # Summary
  if ($type == 1 && $lustOpts =~ /s/) {
    my $headers = '';
    
    if ($reportCltFlag) {
      # 4 different sizes based on whether which value for --lustopts chosen
      # NOTE - order IS critical
      $headers .= "[CLT]Reads${SEP}[CLT]ReadKB${SEP}[CLT]Writes${SEP}[CLT]WriteKB${SEP}";
      $headers .= "[CLTM]Open${SEP}[CLTM]Close${SEP}[CLTM]GAttr${SEP}[CLTM]SAttr${SEP}[CLTM]Seek${SEP}[CLTM]FSync${SEP}[CLTM]DrtHit${SEP}[CLTM]DrtMis${SEP}"
		    if $lustOpts =~ /M/;
      $headers .= "[CLTR]Pend${SEP}[CLTR]Hits${SEP}[CLTR]Misses${SEP}[CLTR]NotCon${SEP}[CLTR]MisWin${SEP}[CLTR]FailGrab${SEP}[CLTR]LckFal${SEP}[CLTR]Discrd${SEP}[CLTR]ZFile${SEP}[CLTR]ZerWin${SEP}[CLTR]RA2Eof${SEP}[CLTR]HitMax${SEP}[CLTR]Wrong${SEP}"
        if $lustOpts =~ /R/;
      if ($lustOpts =~ /B/) {
        foreach my $i (@brwBuckets) {
          $headers .= "[CLTB]r${i}P${SEP}";
        }
        foreach my $i (@brwBuckets) {
          $headers .= "[CLTB]w${i}P${SEP}";
        }
      }
    }
    ${$ref1} .= $headers;
  }

  if ($type == 2 && $lustOpts =~ /d/) {
    if ($reportCltFlag) {
      my $temp = '';
      if ($lustOpts =~ /O/) {  # client OST details
        # we always record I/O in one chunk
        foreach my $inst (@clientOstNames) {
          $temp .= "[CLT:$inst]FileSys${SEP}[CLT:$inst]Ost${SEP}[CLT:$inst]Reads${SEP}[CLT:$inst]ReadKB${SEP}[CLT:$inst]Writes${SEP}[CLT:$inst]WriteKB${SEP}";
        }

        # and if specified, brw stats follow
        if ($lustOpts =~ /B/) {
          foreach my $inst (@clientOstNames) {
            foreach my $j (@brwBuckets) {
              $temp .= "[CLTB:$inst]r${j}P${SEP}"; 
            }
            foreach my $j (@brwBuckets) {
              $temp .= "[CLTB:$inst]w${j}P${SEP}"; 
            }
          }
        }
      } else { # just fs details
        # just like with --lustopts O, these three follow each other in groups
        foreach my $inst (@clientFSNames) {
          $temp .= "[CLT:$inst]FileSys${SEP}[CLT:$inst]Reads${SEP}[CLT:$inst]ReadKB${SEP}[CLT:$inst]Writes${SEP}[CLT:$inst]WriteKB${SEP}";
        }
        if ($lustOpts =~ /M/) {
          foreach my $inst (@clientFSNames) {
            $temp .= "[CLTM:$inst]Open${SEP}[CLTM:$inst]Close${SEP}[CLTM:$inst]GAttr${SEP}[CLTM:$inst]SAttr${SEP}";
            $temp .= "[CLTM:$inst]Seek${SEP}[CLTM:$inst]Fsync${SEP}[CLTM:$inst]DrtHit${SEP}[CLTM:$inst]DrtMis${SEP}";
          }
        }
        if ($lustOpts =~ /R/) {
          foreach my $inst (@clientFSNames) {
            $temp .= "[CLTR:$inst]Pend${SEP}[CLTR:$inst]Hits${SEP}[CLTR:$inst]Misses${SEP}[CLTR:$inst]NotCon${SEP}[CLTR:$inst]MisWin${SEP}[CLTR:$inst]FailGrab${SEP}[CLTR:$inst]LckFal${SEP}";
            $temp .= "[CLTR:$inst]Discrd${SEP}[CLTR:$inst]ZFile${SEP}[CLTR:$inst]ZerWin${SEP}[CLTR:$inst]RA2Eof${SEP}[CLTR:$inst]HitMax${SEP}[CLTR:$inst]WrongMax${SEP}";
          }
        }
      }
      ${$ref1} .= $temp;
    }
  }

  #    D a t a

  # Summary
  if ($type == 3 && $lustOpts =~ /s/) {
    my $plot = '';
      
    if ($reportCltFlag) {
      # There are actually 3 different formats depending on --lustopts
      $plot .= sprintf("$SEP%d$SEP%d$SEP%d$SEP%d",
                       $lustreCltReadTot / $intSecs,
                       $lustreCltReadKBTot / $intSecs,
                       $lustreCltWriteTot / $intSecs, 
                       $lustreCltWriteKBTot / $intSecs);
      $plot .= sprintf("$SEP%d$SEP%d$SEP%d$SEP%d$SEP%d$SEP%d$SEP%d$SEP%d",
                       $lustreCltOpenTot / $intSecs,  
                       $lustreCltCloseTot / $intSecs, 
                       $lustreCltGetattrTot / $intSecs,
                       $lustreCltSetattrTot / $intSecs, 
                       $lustreCltSeekTot / $intSecs,  
                       $lustreCltFsyncTot / $intSecs,  
                       $lustreCltDirtyHitsTot / $intSecs,
                       $lustreCltDirtyMissTot / $intSecs)
        if $lustOpts =~ /M/;
      $plot .= sprintf("$SEP%d$SEP%d$SEP%d$SEP%d$SEP%d$SEP%d$SEP%d$SEP%d$SEP%d$SEP%d$SEP%d$SEP%d$SEP%d",
                       $lustreCltRAPendingTot,
                       $lustreCltRAHitsTot,
                       $lustreCltRAMissesTot, 
                       $lustreCltRANotConTot,   
                       $lustreCltRAMisWinTot,
                       $lustreCltRAFailGrabTot,
                       $lustreCltRAFailLockTot,
                       $lustreCltRAReadDiscTot,
                       $lustreCltRAZeroLenTot, 
                       $lustreCltRAZeroWinTot,
                       $lustreCltRA2EofTot, 
                       $lustreCltRAHitMaxTot,
                       $lustreCltRAWrongTot)
        if $lustOpts =~ /R/;
        
      if ($lustOpts =~ /B/) {
        for (my $i = 0; $i < $numBrwBuckets; $i++) {
          $plot .= sprintf("$SEP%d", $clientRpcReadTot[$i] / $intSecs);
        }
        for (my $i = 0; $i < $numBrwBuckets; $i++) {
          $plot .= sprintf("$SEP%d", $clientRpcWriteTot[$i] / $intSecs);
        }
      }
    }
    ${$ref1} .= $plot;
  }

  # Detail
  if ($type == 4 && $lustOpts =~ /d/) {
    if ($reportCltFlag) {
      my $cltPlot = '';
      if ($lustOpts =~ /O/) { # either OST details or FS details but not both
        foreach my $ostName (@clientOstNames) {
          # when lustre first starts up none of these have values
          my $ost = $clientOstData{$ostName};

          $cltPlot .= sprintf(
            "$SEP%s$SEP%s$SEP%d$SEP%d$SEP%d$SEP%d",
            $ost->{fsName},
            $ost->{ostName},
            $ost->{lun_read}{value} / $intSecs,
            $ost->{lun_readKB}{value} / $intSecs,
            $ost->{lun_write}{value} / $intSecs,
            $ost->{lun_writeKB}{value} / $intSecs);
        }
        if ($lustOpts =~ /B/) {
          foreach my $ostName (@clientOstNames) {
            for (my $j = 0; $j < $numBrwBuckets; $j++) {
              $cltPlot .= 
                sprintf("$SEP%3d", 
                        $clientOstRpcReadData{$ostName}[$j]{value} / $intSecs);
            }
            for (my $j = 0; $j < $numBrwBuckets; $j++) {
              $cltPlot .= 
                sprintf("$SEP%3d",
                        $clientOstRpcReadData{$ostName}[$j]{value} / $intSecs);
            }
          }
        }
      } else { # must be FS
        foreach my $fsName (@clientFSNames) {
          my $fs = $clientFSData{$fsName};

          $cltPlot .= sprintf(
            "$SEP%s$SEP%d$SEP%d$SEP%d$SEP%d",
            $fsName,
            $fs->{read}{value} / $intSecs, 
            $fs->{readKB}{value} / $intSecs,   
            $fs->{write}{value} / $intSecs, 
            $fs->{writeKB}{value} / $intSecs);
        }

        if ($lustOpts =~ /M/) {
          foreach my $fsName (@clientFSNames) {
            my $fs = $clientFSData{$fsName};

            $cltPlot .= 
              sprintf(
                "$SEP%d$SEP%d$SEP%d$SEP%d$SEP%d$SEP%d$SEP%d$SEP%d",
                $fs->{open}{value} / $intSecs,  
                $fs->{close}{value} / $intSecs,  
                $fs->{getattr}{value} / $intSecs,  
                $fs->{setattr}{value} / $intSecs,  
                $fs->{seek}{value} / $intSecs,  
                $fs->{fsync}{value} / $intSecs,  
                $fs->{dirty_pages_hits}{value} / $intSecs,  
                $fs->{dirty_pages_misses}{value} / $intSecs);
          }
        }

        if ($lustOpts =~ /R/) {
          foreach my $fsName (@clientFSNames) {
            $cltPlot .= 
              sprintf("$SEP%d$SEP%d$SEP%d$SEP%d$SEP%d$SEP%d$SEP%d$SEP%d$SEP%d$SEP%d$SEP%d$SEP%d$SEP%d",
                      $lustreCltRAPendingTot,
                      $lustreCltRAHitsTot,  
                      $lustreCltRAMissesTot, 
                      $lustreCltRANotConTot, 
                      $lustreCltRAMisWinTot,
                      $lustreCltRAFailGrabTot,
                      $lustreCltRAFailLockTot,
                      $lustreCltRAReadDiscTot,
                      $lustreCltRAZeroLenTot, 
                      $lustreCltRAZeroWinTot,
                      $lustreCltRA2EofTot, 
                      $lustreCltRAHitMaxTot,
                      $lustreCltRAWrongTot);
          }
        }
      }
      ${$ref1} .= $cltPlot;
    }
  }
}

sub lustreClientPrintExport {
  my $type = shift;
  my $ref1 = shift;
  my $ref2 = shift;
  my $ref3 = shift;
  my $ref4 = shift;
  my $ref5 = shift;

  if ($type eq 'g') {
    if ($lustOpts =~ /s/) {
      if ($cltFlag) {
        push @$ref1, 'lusclt.reads';
        push @$ref2, 'reads/sec';
        push @$ref3, $lustreCltReadTot / $intSecs, 
        push @$ref4, 'Lustre client';
        push @$ref5, undef;

        push @$ref1, 'lusclt.readkbs';
        push @$ref2, 'readkbs/sec';
        push @$ref3, $lustreCltReadKBTot / $intSecs;
        push @$ref4, 'Lustre client';
        push @$ref5, undef;

        push @$ref1, 'lusclt.writes';
        push @$ref2, 'writes/sec';
        push @$ref3, $lustreCltWriteTot / $intSecs;
        push @$ref4, 'Lustre client';
        push @$ref5, undef;

        push @$ref1, 'lusclt.writekbs';
        push @$ref2, 'writekbs/sec';
        push @$ref3, $lustreCltWriteKBTot / $intSecs;
        push @$ref4, 'Lustre client';
        push @$ref5, undef;

        push @$ref1, 'lusclt.numfs';
        push @$ref2, 'filesystems';
        push @$ref3, $numClientFS;
        push @$ref4, 'Lustre client';
        push @$ref5, undef;
      }
    }

    if ($lustOpts =~ /d/) {
      if ($cltFlag) {
        # Either report details by filesystem OR OST
        if ($lustOpts =~ /[MR]/) {
          foreach my $fsName (@clientFSNames) {
            my $fs = $clientFSData{$fsName};

            push @$ref1, "lusost.reads.$fsName";
            push @$ref2, 'reads/sec';
            push @$ref3, $fs->{read}{value} / $intSecs;
            push @$ref4, 'Lustre client';
            push @$ref5, undef;
            
            push @$ref1, "lusost.readkbs.$fsName";
            push @$ref2, 'readkbs/sec';
            push @$ref3, $fs->{readKB}{value} / $intSecs;
            push @$ref4, 'Lustre client';
            push @$ref5, undef;
            
            push @$ref1, "lusost.readSizeKB.$fsName";
            push @$ref2, 'readSizeKB';
            push @$ref3, $fs->{read}{value} ? $fs->{readKB}{value} / $fs->{read}{value} : 0;
            push @$ref4, 'Lustre client';
            push @$ref5, undef;
            
            push @$ref1, "lusost.writes.$fsName";
            push @$ref2, 'writes/sec';
            push @$ref3, $fs->{write}{value} / $intSecs;
            push @$ref4, 'Lustre client';
            push @$ref5, undef;
            
            push @$ref1, "lusost.writekbs.$fsName";
            push @$ref2, 'writekbs/sec';
            push @$ref3, $fs->{writeKB}{value} / $intSecs;
            push @$ref4, 'Lustre client';
            push @$ref5, undef;
            
            push @$ref1, "lusost.writeSizeKB.$fsName";
            push @$ref2, 'writeSizeKB';
            push @$ref3, $fs->{write}{value} ? $fs->{writeKB}{value} / $fs->{write}{value} : 0;
            push @$ref4, 'Lustre client';
            push @$ref5, undef;
          }
        }

        if ($lustOpts =~ /O/) {
          foreach my $ostFullName (@clientOstNames) {
            my $ost = $clientOstData{$ostFullName};
            my $fsName = $ost->{fsName};
            my $ostName = $ost->{ostName};
            
            push @$ref1, "lusost.reads.$fsName-$ostName";
            push @$ref2, 'reads/sec', 
            push @$ref3, $ost->{lun_read}{value} / $intSecs;
            push @$ref4, 'Lustre client OST Reads';
            push @$ref5, undef;
            
            push @$ref1, "lusost.readkbs.$fsName-$ostName";
            push @$ref2, 'readkbs/sec';
            push @$ref3, $ost->{lun_readKB}{value} / $intSecs;
            push @$ref4, 'Lustre client OST Reads';
            push @$ref5, undef;
            
            push @$ref1, "lusost.writes.$fsName-$ostName";
            push @$ref2, 'writes/sec';
            push @$ref3, $ost->{lun_write}{value} / $intSecs;
            push @$ref4, 'Lustre client OST Writes';
            push @$ref5, undef;
            
            push @$ref1, "lusost.writekbs.$fsName-$ostName";
            push @$ref2, 'writekbs/sec';
            push @$ref3, $ost->{lun_writeKB}{value} / $intSecs;
            push @$ref4, 'Lustre client OST Writes';
            push @$ref5, undef;
          }
        }

        if ($lustOpts =~ /M/) {
          foreach my $fsName (@clientFSNames) {
            my $fs = $clientFSData{$fsName};
            
            push @$ref1, "lusost.open.$fsName";
            push @$ref2, 'opens/sec';
            push @$ref3, $fs->{open}{value};
            push @$ref4, 'Lustre client';
            push @$ref5, undef;
            
            push @$ref1, "lusost.close.$fsName";
            push @$ref2, 'closes/sec';
            push @$ref3, $fs->{close}{value} / $intSecs;
            push @$ref4, 'Lustre client';
            push @$ref5, undef;
            
            push @$ref1, "lusost.getattr.$fsName";
            push @$ref2, 'getattrs/sec';
            push @$ref3, $fs->{getattr}{value} / $intSecs;
            push @$ref4, 'Lustre client';
            push @$ref5, undef;
            
            push @$ref1, "lusost.setattr.$fsName";
            push @$ref2, 'setattrs/sec';
            push @$ref3, $fs->{setattr}{value} / $intSecs;
            push @$ref4, 'Lustre client';
            push @$ref5, undef;
            
            push @$ref1, "lusost.seek.$fsName";
            push @$ref2, 'seeks/sec';
            push @$ref3, $fs->{seek}{value} / $intSecs;
            push @$ref4, 'Lustre client';
            push @$ref5, undef;
            
            push @$ref1, "lusost.fsync.$fsName";
            push @$ref2, 'fsyncs/sec';
            push @$ref3, $fs->{fsync}{value} / $intSecs;
            push @$ref4, 'Lustre client';
            push @$ref5, undef;
            
            push @$ref1, "lusost.dirtyhits.$fsName";
            push @$ref2, 'dirtyhits/sec';
            push @$ref3, $fs->{dirty_pages_hits}{value} / $intSecs;
            push @$ref4, 'Lustre client';
            push @$ref5, undef;
            
            push @$ref1, "lusost.dirtymiss.$fsName";
            push @$ref2, 'dirtymisses/sec';
            push @$ref3, $fs->{dirty_pages_misses}{value} / $intSecs;
            push @$ref4, 'Lustre client';
            push @$ref5, undef;
          }
        }
        
        if ($lustOpts =~ /R/) {
          foreach my $fsName (@clientFSNames) {
            my $fs = $clientFSData{$fsName};
            
            push @$ref1, "lusost.pending.$fsName";
            push @$ref2, 'pending/sec';
            push @$ref3, $fs->{ra_pending}{value} / $intSecs;
            push @$ref4, 'Lustre client';
            push @$ref5, undef;
            
            push @$ref1, "lusost.prefetchhits.$fsName";
            push @$ref2, 'prefetchhits/sec';
            push @$ref3, $fs->{ra_hits}{value} / $intSecs;
            push @$ref4, 'Lustre client';
            push @$ref5, undef;
            
            push @$ref1, "lusost.prefetchmisses.$fsName";
            push @$ref2, 'prefetchmisses/sec';
            push @$ref3, $fs->{ra_misses}{value} / $intSecs;
            push @$ref4, 'Lustre client';
            push @$ref5, undef;
            
            push @$ref1, "lusost.notcon.$fsName";
            push @$ref2, 'notcon/sec';
            push @$ref3, $fs->{ra_notcon}{value} / $intSecs;
            push @$ref4, 'Lustre client';
            push @$ref5, undef;
            
            push @$ref1, "lusost.miswin.$fsName";
            push @$ref2, 'miswin/sec';
            push @$ref3, $fs->{ra_miswin}{value} / $intSecs;
            push @$ref4, 'Lustre client';
            push @$ref5, undef;
            
            push @$ref1, "lusost.failgrab.$fsName";
            push @$ref2, 'falgrab/sec';
            push @$ref3, $fs->{ra_failgrab}{value} / $intSecs;
            push @$ref4, 'Lustre client';
            push @$ref5, undef;
            
            push @$ref1, "lusost.faillock.$fsName";
            push @$ref2, '1/sec';
            push @$ref3, $fs->{ra_faillock}{value} / $intSecs;
            push @$ref4, 'Lustre client';
            push @$ref5, undef;
            
            push @$ref1, "lusost.readdisc.$fsName";
            push @$ref2, 'readdisk/sec';
            push @$ref3, $fs->{ra_readdisc}{value} / $intSecs;
            push @$ref4, 'Lustre client';
            push @$ref5, undef;
            
            push @$ref1, "lusost.zerolen.$fsName";
            push @$ref2, 'zerolen/sec';
            push @$ref3, $fs->{ra_zerolen}{value} / $intSecs;
            push @$ref4, 'Lustre client';
            push @$ref5, undef;
            
            push @$ref1, "lusost.zerowin.$fsName";
            push @$ref2, 'zerowin/sec';
            push @$ref3, $fs->{ra_zerowin}{value} / $intSecs;
            push @$ref4, 'Lustre client';
            push @$ref5, undef;

            push @$ref1, "lusost.ra2eof.$fsName";
            push @$ref2, 'ra2eof/sec';
            push @$ref3, $fs->{ra_2EofMax}{value} / $intSecs;
            push @$ref4, 'Lustre client';
            push @$ref5, undef;
            
            push @$ref1, "lusost.hitmax.$fsName";
            push @$ref2, 'hitmax/sec';
            push @$ref3, $fs->{ra_hitmax}{value} / $intSecs;
            push @$ref4, 'Lustre client';
            push @$ref5, undef;
            
            push @$ref1, "lusost.wrong.$fsName";
            push @$ref2, 'wrong/sec';
            push @$ref3, $fs->{ra_wrong}{value} / $intSecs;
            push @$ref4, 'Lustre client';
            push @$ref5, undef;
          }
        }
        
        if ($lustOpts =~ /B/) {
          foreach my $ostFullName (@clientOstNames) {
            my $ost = $clientOstData{$ostFullName};
            my $fsName = $ost->{fsName};
            my $ostName = $ost->{ostName};

            for (my $j = 0; $j < $numBrwBuckets; $j++) {          	
              push @$ref1, "lusost.readHist.$fsName-$ostName.$brwBuckets[$j]k";
              push @$ref2, 'reads/sec';
              push @$ref3, $clientOstRpcReadData{$ostName}[$j]{value} / $intSecs;
              push @$ref4, 'Lustre client OST Reads';
              push @$ref5, undef;
              
              push @$ref1, "lusost.writeHist.$fsName-$ostName.$brwBuckets[$j]k";
              push @$ref2, 'writes/sec';
              push @$ref3, $clientOstRpcWriteData{$ostName}[$j]{value} / $intSecs;
              push @$ref4, 'Lustre client OST Writes';
              push @$ref5, undef;
            }
          }
        }
      }
    }
  } elsif ($type eq 'l') {
    if ($lustOpts =~ /s/) {
      if ($cltFlag) {
        push @$ref1, "lusclt.reads";
        push @$ref2, $lustreCltReadTot / $intSecs;
        
        push @$ref1, "lusclt.readkbs";
        push @$ref2, $lustreCltReadKBTot / $intSecs;
        
        push @$ref1, "lusclt.writes";
        push @$ref2, $lustreCltWriteTot / $intSecs;

        push @$ref1, "lusclt.writekbs";
        push @$ref2, $lustreCltWriteKBTot / $intSecs;

        push @$ref1, "lusclt.numfs";
        push @$ref2, $numClientFS;
      }
    }
  } elsif ($type eq 's') {
    if ($lustOpts =~ /s/) {
      my $pad = $XCFlag ? '  ' : '';

      if ($cltFlag) {
        my ($reads, $readKBs, $writes, $writeKBs) = (0, 0, 0, 0);
        foreach my $fsName (@clientFSNames) {
          my $fs = $clientFSData{$fsName};
          $reads += $fs->{read}{last};
          $readKBs += $fs->{readKB}{last};
          $writes += $fs->{write}{last};
          $writeKBs += $fs->{writeKB}{last};
        }
        $$ref1 .= "$pad(lusclt (reads $reads) (readkbs $readKBs) (writes $writes) (writekbs $writeKBs) (numfs $numClientFS))\n";
      }
    }
  }
}

1;
