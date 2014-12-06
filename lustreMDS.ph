# copyright, 2014 Terascala, Inc. All rights reserved
#
# lustreMDS may be copied only under the terms of either the Artistic License
# or the GNU General Public License

# Lustre MDS Data Collector

use strict;

# Allow reference to collectl variables, but be CAREFUL as these should be treated as readonly
our ($miniFiller, $rate, $SEP, $datetime, $intSecs, $totSecs, $showColFlag);
our ($firstPass, $debug, $filename, $playback, $ioSizeFlag, $verboseFlag);
our ($OneKB, $OneMB, $OneGB, $TenGB);
our ($miniDateTime, $options, $FS, $Host, $XCFlag, $interval, $count);
our ($sameColsFlag, $subsys, $ReqDir);

require "$ReqDir/LustreSingleton.pm";

# Global to this module
my $lustOpts = undef;
my $lustOptsOnly = undef;
my $lustre_singleton = new LustreSingleton();
my $lustre_version = $lustre_singleton->getVersion();
my $METRIC = {value => 0, last => 0};
my $SUM_METRIC = {value => 0, lastCumulcount => undef, lastSum => undef};
my $printMsg = $debug & 16384;

my @clientNames = (); # Ordered list of MDS/OSS clients
my %clientNamesHash = ();
my $clientNamesStr = ''; # Concatenated string of client names
my $numClients = 0; # Number of clients accessing this MDS/OSS

my $numMdt = 0; # Number of metadata targets
my $mdsFlag = 0;
my $reportMdsFlag = 0;
my $mdtNamesStr = ''; # Concatenated string of MDT names
my %mdsData = (); # MDS performance metrics
my %mdsClientData = (); # MDS client access metrics indexed by client id

my $mdsGetattrPlusTOT = 0;
my $mdsSetattrPlusTOT = 0;
my $mdsSyncTOT = 0;
my $mdsReintTOT = 0;
my $mdsReintUnlinkTOT = 0;
my $mdsFileCreateTOT = 0;

my $limLusReints = 1000;
my $NO_SAMPLE = -12341234;

if ($printMsg) {
  if (defined($lustre_version)) {
    logmsg('I', "Lustre version $lustre_version");
  } else {
    logmsg('I', "Lustre version currently unknown");
  }
}

sub getMdtDir {
  my ($baseDir) = @_;
  my $mdt_dir = '';
  my @mdtStatDir = glob("$baseDir/*");
  foreach my $item (@mdtStatDir) {
    if ($item =~ m/\/num_refs$/) { next; }
    if ($item =~ m/\/.*-MDT\d+$/) { 
      $mdt_dir = $item;
      last; 
    }
  }
  print "mdt_dir: $mdt_dir\n" if $printMsg;
  return $mdt_dir;
}

sub createMdt {
  my $mdt = {close => {%{$METRIC}},
	     connect => {%{$METRIC}},
             disconnect => {%{$METRIC}},
             file_create => {%{$METRIC}},
	     getattr => {%{$METRIC}},
             getattr_lock => {%{$METRIC}},
             getxattr => {%{$METRIC}},
             mkdir => {%{$METRIC}},
             open => {%{$METRIC}},
             reint => {%{$METRIC}},
             reint_create => {%{$METRIC}},
             reint_link => {%{$METRIC}},
             reint_rename => {%{$METRIC}},
             reint_setattr => {%{$METRIC}},
             reint_unlink => {%{$METRIC}},
             req_active => {%{$SUM_METRIC}},
             req_qdepth => {%{$SUM_METRIC}},
	     req_timeout => {%{$SUM_METRIC}},
             req_waittime => {%{$SUM_METRIC}},
	     reqbuf_avail => {%{$SUM_METRIC}},
             rmdir => {%{$METRIC}},
             setattr => {%{$METRIC}},
             setxattr => {%{$METRIC}},
             statfs => {%{$METRIC}},
             sync => {%{$METRIC}},
             unlink => {%{$METRIC}}
  };             
  return $mdt;
}

sub createMdsClient {
  my $mdsClient = {file_create => {%{$METRIC}},
                   unlink => {%{$METRIC}},
                   getattr => {%{$METRIC}},
                   setattr => {%{$METRIC}},
                   open => {%{$METRIC}},
                   close => {%{$METRIC}}};
  return $mdsClient;
}

sub lustreCheckMdsNew {
  $lustre_version = $lustre_singleton->getVersion()
    if (!defined($lustre_version));

  return 0 if (!defined($lustre_version));

  error("Lustre versions earlier than 1.8.0 are not currently supported")
    if ($lustre_version lt '1.8.0');

  my $mdtDir = '/proc/fs/lustre/';
  $mdtDir .= ($lustre_version ge '2.1.1') ? 'mdt' : 'mds';
  
  return 0 if ($numMdt == 0) && !-e $mdtDir;

  my $saveMdtNamesStr = $mdtNamesStr;
  my $saveNumMdt = $numMdt;
  
  $mdtNamesStr = '';
  $numMdt = 0;
  $mdsFlag = $reportMdsFlag = 0;
  
  my @mdtDirs = glob("$mdtDir/*");
  foreach my $mdtName (@mdtDirs) {
    next if $mdtName =~ /num_refs/;
    $mdtName = basename($mdtName);
    $mdtNamesStr .= "$mdtName ";
    $numMdt++;
    $mdsFlag = $reportMdsFlag = 1;
  }
  $mdtNamesStr =~ s/ $//;
  
  print "numMdt: $numMdt\n" .
    "mdtNames: $saveMdtNamesStr | $mdtNamesStr\n" if $printMsg;
  
  if ($numMdt > 0) { 
    if ($saveNumMdt == 0) {
      logmsg("I", "Adding MDT") if $printMsg;
      my $mdt = createMdt();
      %mdsData = %{$mdt};
    }
  } else {
    if ($saveNumMdt > 0) {
      logmsg("I", "Removing MDT") if $printMsg;
      %mdsData = ();
    }
  }
  
  if ($lustOpts =~ /C/) {
    my @saveClientNames = @clientNames;
    my $saveClientNamesStr = $clientNamesStr;
    
    %clientNamesHash = ();
    @clientNames = ();
    $numClients = 0;
    
    my $exportsDir = "$mdtDir/$mdtNamesStr/exports";
    my @clientDirs = glob("$exportsDir/*");
    foreach my $client (@clientDirs) {
      next if $client =~ /clear/;
        
      $client = basename($client);
      $clientNamesHash{$client} = $client;
      push(@clientNames, $client);
      $numClients++;
      
      # check if client is newly added
      if (! exists($mdsClientData{$client})) {
        logmsg('I', "Adding MDS client $client") if $printMsg;
        $mdsClientData{$client} = createMdsClient();
      }
    }
    $clientNamesStr = join(' ', @clientNames);

    if ($clientNamesStr ne $saveClientNamesStr) {
      # Remove stats for clients that no longer exist
      foreach my $client (@saveClientNames) {
        if (! exists($clientNamesHash{$client})) {
          logmsg("I", "Deleting MDT client $client") if $printMsg;
          delete $mdsClientData{$client};
        }
      }
    }
    
    if ($printMsg) {
      print "numClients: $numClients\n" .
        "Lustre clients: $clientNamesStr\n";
    }
  }
  
  # Change info is important even when not logging except during initialization
  if ($mdtNamesStr ne $saveMdtNamesStr) {
    my $comment = ($filename eq '') ? '#' : '';
    my $text = "Lustre MDS FS Changed -- Old: $saveMdtNamesStr  New: $mdtNamesStr";
    logmsg('W', "${comment}$text") if !$firstPass;
    print "$text\n" if $firstPass && $printMsg;
  }
  return ($mdtNamesStr ne $saveMdtNamesStr) ? 1 : 0;
}

sub lustreGetRpcStats {
  my $proc = shift;

  if (!open PROC, "<$proc") {
    return(0);
  }
  while (my $line = <PROC>) {
    if (($line =~ /^req_waittime /) ||
        ($line =~ /^req_qdepth /) ||
        ($line =~ /^req_active /) ||
        ($line =~ /^req_timeout /) ||
        ($line =~ /^reqbuf_avail /)) {
      record(2, "MDS_RPC $line" ); 
    }
  }
  close PROC;
  return(1);
}

sub lustreGetMdtStats {
  my $proc_mdt = shift;
  my $proc_mds = shift;
  my %lines = map {$_ => undef} qw(connect 
                                   close 
                                   disconnect
	                           file_create
	                           getattr
	                           getattr_lock
	                           getxattr
	                           mkdir
	                           open
	                           reint
	                           reint_create
	                           reint_link
	                           reint_rename
	                           reint_setattr
	                           reint_unlink
	                           rmdir
	                           setattr
	                           setxattr
	                           statfs
	                           sync
                                   unlink);

  if (open PROC, "<$proc_mds") {
    while (my $line = <PROC>) {
      if ($line =~ /^mds_(\S+) /) {
	my $metric = $1;
	if (exists $lines{$metric}) {
	  # Remove mds_
	  substr($line, 0, 4) = "";
	  $lines{$metric} = $line;
	}
      }
    }
    close PROC;
  }

  # mdt <metric> values supercede those from the mds file
  if (open PROC, "<$proc_mdt") {
    while (my $line = <PROC>) {
      if ($line =~ /^(\S+) /) {
	my $metric = $1;
	if (exists $lines{$metric}) {
	  $lines{$metric} = $line;
	}
      }
    }
  }
  close PROC;

  while (my ($metric, $line) = each %lines) {
    record(2, "MDS $line") if defined $line; 
  }

  return(1);
}

sub lustreGetMdtClientStats {
  my $client = shift;
  my $proc = shift;
  
  return(0) if (!open PROC, "<$proc");

  while (my $line = <PROC>) {
    if (($line =~ /^getattr /) ||
	($line =~ /^setattr /) ||
        ($line =~ /^file_create /) ||
        ($line =~ /^unlink /) ||
        ($line =~ /^open /) ||
        ($line =~ /^close /)) {
      record(2, "MDS LCL-$client" . "-$line"); 
    }
  }
  close PROC;
  return(1);
}

sub lustreGetMdsStats {
  # collect stats from the MDS
  my $mds_stats_file = '';
  my $mdt_stats_file = '';
  my $mds_rpc_stats_file = '';
  my $mds_client_stats_dir = '';
  if ($lustre_version ge '2.5.0') {
    my $mdt_dir = getMdtDir("/proc/fs/lustre/mdt");
        
    $mds_stats_file = "/proc/fs/lustre/mds/MDS/mdt/stats";
    $mdt_stats_file = "$mdt_dir/md_stats";
    $mds_rpc_stats_file = "/proc/fs/lustre/mds/MDS/mdt/stats";
    $mds_client_stats_dir = "$mdt_dir/exports";
  } elsif ($lustre_version ge '2.1.1') { 
    my $mdt_dir = getMdtDir("/proc/fs/lustre/mdt");
        
    $mds_stats_file = "$mdt_dir/mdt/stats";
    $mdt_stats_file = "$mdt_dir/md_stats";
    $mds_rpc_stats_file = "$mdt_dir/mdt/stats";
    $mds_client_stats_dir = "$mdt_dir/exports";
  } elsif ($lustre_singleton->getVersion() ge '1.8.8') {
    my $mdt_dir = getMdtDir("/proc/fs/lustre/mds");

    $mds_stats_file = "/proc/fs/lustre/mdt/MDS/mds/stats";
    $mdt_stats_file = "$mdt_dir/stats";
    $mds_rpc_stats_file = "/proc/fs/lustre/mdt/MDS/mds/stats";
    $mds_client_stats_dir = "$mdt_dir/exports";
  } else {
    my $mdt_dir = getMdtDir("/proc/fs/lustre/mds");

    $mds_stats_file = "/proc/fs/lustre/mdt/MDS/mds/stats";
    $mdt_stats_file = "$mdt_dir/stats";
    $mds_rpc_stats_file = $mds_stats_file;
    $mds_client_stats_dir = "$mdt_dir/exports";
  }

  lustreGetRpcStats("$mds_rpc_stats_file");

  lustreGetMdtStats($mdt_stats_file, $mds_stats_file);

  foreach my $client (@clientNames) {
    print "client stats file: $mds_client_stats_dir/exports/$client/stats\n"
      if $printMsg; 
    lustreGetMdtClientStats($client, "$mds_client_stats_dir/$client/stats");
  }
  return(1);
}

sub lustreMDSInit {
  my $impOptsref = shift;
  my $impKeyref = shift;

  error("You must remove the -sl or -sL option to use this plugin")
    if ($subsys =~ /l/i);

  $lustOpts = ${$impOptsref};
  error('Valid lustre options are: s C') 
    if defined($lustOpts) && $lustOpts !~ /^[sC]*$/;

  $lustOpts = 's' if !defined($lustOpts);
  ${impOptsref} = $lustOpts;

  error("Lustre versions earlier than 1.8.0 are not currently supported")
    if (defined($lustre_version) && $lustre_version lt '1.8.0');

  print "lustreMDSInit: options: $lustOpts\n" if $printMsg;

  ${$impKeyref} = 'MDS';

  lustreCheckMdsNew();

  $lustOptsOnly = $lustOpts;
  $lustOptsOnly =~ s/[s]//;

  return(1);
}

sub lustreMDSUpdateHeader {
  my $lineref = shift;

  ${$lineref} .= 
    "# Lustre MDS Data Collector: Version 1.0, Lustre version: $lustre_version\n";
}

sub lustreMDSGetData {
  lustreGetMdsStats() if ($numMdt > 0);
}

sub lustreMDSInitInterval {
  # Check to see if any services changed and if they did, we may need
  # a new logfile as well.
  newLog($filename, "", "", "", "", "")
    if lustreCheckMdsNew() && $filename ne '';

  $sameColsFlag = 0 if (length($lustOptsOnly) > 1) || $verboseFlag;
}

sub delta {
  my $current = shift;
  my $last = shift;

  return (defined $last && ($current > $last)) ? $current - $last : 0;
}

sub updateSumMetric {
  my $metric = shift;
  my $cumulCount = shift;
  my $sum = shift;

  if (defined $metric->{lastCumulCount} && defined $metric->{lastSum}) {
    if ($cumulCount == $metric->{lastCumulCount}) {
      $metric->{value} = $NO_SAMPLE;
    } elsif ($cumulCount > $metric->{lastCumulCount} &&
	$sum >= $metric->{lastSum}) {
      $metric->{value} = ($sum - $metric->{lastSum}) / 
	  ($cumulCount - $metric->{lastCumulCount});
    }
  } else {
    $metric->{value} = 0;
  }
  $metric->{lastCumulCount} = $cumulCount;
  $metric->{lastSum} = $sum;
}

sub lustreMDSAnalyze {
  my $type = shift;
  my $dataref = shift;
  my $data = ${$dataref};

  logmsg('I', "lustreMDSAnalyze: type: $type, data: $data") if $printMsg;

  if ($lustOpts =~ /s/ && $type =~ /MDS_RPC/) {
    my ($metric, $cumulCount, $sum) = (split(/\s+/, $data))[0, 1, 6];
    my $attr = $mdsData{$metric};
    updateSumMetric($attr, $cumulCount, $sum);
  } elsif ($lustOpts =~ /s/ && $type =~ /MDS/) {
    my ($metric, $value) = (split(/\s+/, $data))[0,1];

    if ($metric =~ /^LCL/) {
      my ($client, $metric) = (split("-", $metric))[1, 2];
      
      print "metric: $metric, value: $value\n" if $printMsg;

      my $attr = $mdsClientData{$client}{$metric};
      $attr->{value} = delta($value, $attr->{last});
      $attr->{last} = $value;
    } else {
      my $attr = $mdsData{$metric};
      $attr->{value} = delta($value, $attr->{last});
      $attr->{last} = $value;
    }
  }
}

# This and the 'print' routines should be self explanitory as they pretty much simply
# return a string in the appropriate format for collectl to dispose of.
sub lustreMDSPrintBrief {
  my $type = shift;
  my $lineref = shift;
  
  if ($type == 1) { # header line 1
    ${$lineref} .= "<-----------Lustre MDS----------->" 
      if $lustOpts =~ /s/ && $reportMdsFlag;
  } elsif ($type == 2) { # header line 2
    if ($lustOpts =~ /s/ && $reportMdsFlag) {
      ${$lineref} .= "Gattr+ Sattr+   Sync  ";
      ${$lineref} .= 'Unlnk ';
      ${$lineref} .= "Create ";
    }
  } elsif ($type == 3) { # data
    # MDS
    if ($lustOpts =~ /s/ && $reportMdsFlag) {
      my $setattrPlus = $mdsData{reint_setattr}{value} +
	$mdsData{setattr}{value} +
        $mdsData{setxattr}{value};
      my $getattrPlus = 
        $mdsData{getattr}{value} + 
        $mdsData{getattr_lock}{value} + 
        $mdsData{getxattr}{value};

      my $delete = $mdsData{reint_unlink}{value} +
	$mdsData{unlink}{value} +
	$mdsData{rmdir}{value};

      my $create = $mdsData{file_create}{value} + 
	$mdsData{mkdir}{value} +
	$mdsData{reint_create}{value};

      ${$lineref} .= sprintf("%6s %6s %6s %6s %6s",
                             cvt($getattrPlus / $intSecs, 6),
                             cvt($setattrPlus / $intSecs, 6),
                             cvt($mdsData{sync}{value} / $intSecs, 6),
                             cvt($delete / $intSecs, 6), 
                             cvt($create / $intSecs, 6));
    }
  } elsif ($type == 4) { # reset 'total' counters
    $mdsGetattrPlusTOT = 0;
    $mdsSetattrPlusTOT = 0;
    $mdsSyncTOT = 0;
    $mdsReintTOT = 0;
    $mdsReintUnlinkTOT = 0;
    $mdsFileCreateTOT = 0;
  } elsif ($type == 5) { # increment 'total' counters
    if ($numMdt) {
      $mdsGetattrPlusTOT += 
        $mdsData{getattr}{value} + 
        $mdsData{getattr_lock}{value} + 
        $mdsData{getxattr}{value};
      $mdsSetattrPlusTOT += $mdsData{reint_setattr}{value} +
	$mdsData{setattr}{value} +
        $mdsData{setxattr}{value};
      $mdsSyncTOT += $mdsData{sync}{value};
      $mdsReintTOT += $mdsData{reint}{value};
      $mdsReintUnlinkTOT += $mdsData{reint_unlink}{value};
      $mdsFileCreateTOT += $mdsData{file_create}{value};
    }
  } elsif ($type == 6) { # print 'total' counters
    # Since this never goes over a socket we can just do a simple print.
    if ($lustOpts =~ /s/ && $reportMdsFlag) {
      printf "%6s %6s %6s %6s %6s",
      cvt($mdsGetattrPlusTOT / $totSecs, 6),
      cvt($mdsSetattrPlusTOT / $totSecs, 6),
      cvt($mdsSyncTOT / $totSecs, 6),
      cvt($mdsReintUnlinkTOT / $totSecs, 6),
      cvt($mdsFileCreateTOT / $totSecs, 6);
    }
  }
}

sub sumMetricValStr {
  my $val = shift;
  my $width = shift;

  return ($val == $NO_SAMPLE) ? 
      sprintf("%".$width."s", " ") : 
      sprintf("%".$width."d", $val);
}

sub lustreMDSPrintVerbose {
  my $printHeader = shift;
  my $homeFlag = shift;
  my $lineref = shift;

  # Note that last line of verbose data (if any) still sitting in $$lineref
  my $line = ${$lineref} = '';
  
  # This is the normal output for an MDS
  if ($lustOpts =~ /s/ && $reportMdsFlag) {
    my $line = '';
    $line .= "\n" if !$homeFlag;
    $line .= "# LUSTRE MDS SUMMARY ($rate)\n";
    $line .= "#${miniDateTime} Getattr GttrLck  StatFS    Sync  Gxattr  Sxattr Connect Disconn";
    $line .= " Create   Link Setattr Rename Unlink";
    $line .= " FCreate    Open   Close   Mkdir   Rmdir" 
        if ($lustre_version ge '1.8.8');
    $line .= "\n";
    ${$lineref} .= $line;
    exit if $showColFlag;

    # Don't report if exception processing in effect and we're below limit
    # NOTE - exception processing only for versions < 1.6.5
    $line = '';
    if ($options !~ /x/ || 
        $mdsData{reint}{value} / $intSecs >= $limLusReints) {
      $line .= sprintf("$datetime  %7d %7d %7d %7d %7d %7d %7d %7d",
                       $mdsData{getattr}{value} / $intSecs, 
                       $mdsData{getattr_lock}{value} / $intSecs,
                       $mdsData{statfs}{value} / $intSecs,
                       $mdsData{sync}{value} / $intSecs,
                       $mdsData{getxattr}{value} / $intSecs,
                       $mdsData{setxattr}{value} / $intSecs,
                       $mdsData{connect}{value} / $intSecs,
                       $mdsData{disconnect}{value} / $intSecs);
      
      $line .= sprintf(" %6d %6d %7d %6d %6d",
                       $mdsData{reint_create}{value} / $intSecs, 
                       $mdsData{reint_link}{value} / $intSecs, 
                       ($mdsData{reint_setattr}{value} +
			$mdsData{setattr}{value}) / $intSecs, 
                       $mdsData{reint_rename}{value} / $intSecs,
                       ($mdsData{reint_unlink}{value} + 
			$mdsData{unlink}{value}) / $intSecs);
                       
      $line .= sprintf(" %7d %7d %7d %7d %7d",
                       $mdsData{file_create}{value} / $intSecs, 
                       $mdsData{open}{value} / $intSecs, 
                       $mdsData{close}{value} / $intSecs, 
                       $mdsData{mkdir}{value} / $intSecs,
                       $mdsData{rmdir}{value} / $intSecs) 
        if ($lustre_version ge '1.8.8');
    }
    $line .= "\n";
    ${$lineref} .= $line;

    $line = '';
    $line .= "\n";
    $line .= "# LUSTRE MDS RPC SUMMARY (Normal ops)\n";
    $line .= "#${miniDateTime} Active Queue Waittime Timeout Available";
    $line .= "\n";
    ${$lineref} .= $line;
    $line = '';
    $line .= "#${miniDateTime}        Depth  (usec)   (sec)   Buffers";
    $line .= "\n";
    ${$lineref} .= $line;

    my $req_active = sumMetricValStr($mdsData{req_active}{value}, 6);
    my $req_qdepth = sumMetricValStr($mdsData{req_qdepth}{value}, 5);
    my $req_waittime = sumMetricValStr($mdsData{req_waittime}{value}, 8);
    my $req_timeout = sumMetricValStr($mdsData{req_timeout}{value}, 7);
    my $reqbuf_avail = sumMetricValStr($mdsData{reqbuf_avail}{value}, 9);

    $line = '';
    $line .= $datetime . "  " . $req_active . " " . $req_qdepth . " " .
	$req_waittime . " " . $req_timeout . " " . $reqbuf_avail;
    $line .= "\n";
    ${$lineref} .= $line;
  }
}

# Just be sure to use $SEP in the right places.  A simple trick to make sure you've done it
# correctly is to generste a small plot file and load it into a speadsheet, making sure each
# column of data has a header and that they aling 1:1.
sub lustreMDSPrintPlot {
  my $type = shift;
  my $ref1 = shift;

  #    H e a d e r s

  # Summary
  if ($type == 1 && $lustOpts =~ /s/) {
    my $headers = '';
    if ($reportMdsFlag) {
      $headers .= "[MDS]Getattr${SEP}[MDS]GetattrLock${SEP}[MDS]Statfs${SEP}[MDS]Sync${SEP}";
      $headers .= "[MDS]Getxattr${SEP}[MDS]Setxattr${SEP}[MDS]Connect${SEP}[MDS]Disconnect${SEP}";
      $headers .= "[MDS]Reint${SEP}[MDS]Create${SEP}[MDS]Link${SEP}[MDS]Setattr${SEP}";
      $headers .= "[MDS]Rename${SEP}[MDS]Unlink${SEP}[MDS]FileCreate${SEP}";
    }
    
    ${$ref1} .= $headers;
  }

  if ($type == 2 && $lustOpts =~ /d/) {
  }

  #    D a t a

  # Summary
  if ($type == 3 && $lustOpts =~ /s/) {
    my $plot = '';
    # MDS goes first since for detail, the OST is variable and if we ever
    # do both we want consistency of order.  Also note that by reporting all 6
    # reints we assure consisency across lustre versions
    if ($reportMdsFlag) {
      $plot .= sprintf("$SEP%$FS$SEP%$FS$SEP%$FS$SEP%$FS$SEP%$FS$SEP%$FS$SEP%$FS$SEP%$FS$SEP%$FS$SEP%$FS$SEP%$FS$SEP%$FS$SEP%$FS$SEP%$FS$SEP%$FS",
                       $mdsData{getattr}{value} / $intSecs, 
                       $mdsData{getattr_lock}{value} / $intSecs,
                       $mdsData{statfs}{value} / $intSecs, 
                       $mdsData{sync}{value} / $intSecs,
                       $mdsData{getxattr}{value} / $intSecs,
                       $mdsData{setxattr}{value} / $intSecs,
                       $mdsData{connect}{value} / $intSecs,
                       $mdsData{disconnect}{value} / $intSecs,
                       $mdsData{reint}{value} / $intSecs,
                       $mdsData{reint_create}{value} / $intSecs, 
                       $mdsData{reint_link}{value} / $intSecs, 
                       ($mdsData{reint_setattr}{value} +
			$mdsData{setattr}{value}) / $intSecs,
                       $mdsData{reint_rename}{value} / $intSecs,
                       ($mdsData{reint_unlink}{value} +
			$mdsData{unlink}{value}) / $intSecs,
                       $mdsData{file_create}{value} / $intSecs);
    }

    ${$ref1} .= $plot;
  }

  # Detail
  if ($type == 4 && $lustOpts =~ /d/) {
  }
}

sub lustreMDSPrintExport {
  my $type = shift;
  my $ref1 = shift;
  my $ref2 = shift;
  my $ref3 = shift;
  my $ref4 = shift;
  my $ref5 = shift;

  if ($type eq 'g') {
    if ($lustOpts =~ /s/) {
      if ($mdsFlag) {
	if ($mdsData{req_waittime}{value} != $NO_SAMPLE) {
	  push @$ref1, 'lusmds.waittime';
	  push @$ref2, 'usec';
	  push @$ref3, $mdsData{req_waittime}{value};
	  push @$ref4, 'Lustre MDS RPC';
	  push @$ref5, 'Request Wait Time';
	}

	if ($mdsData{req_qdepth}{value} != $NO_SAMPLE) {
	  push @$ref1, 'lusmds.qdepth';
	  push @$ref2, 'queue depth';
	  push @$ref3, $mdsData{req_qdepth}{value};
	  push @$ref4, 'Lustre MDS RPC';
	  push @$ref5, 'Request Queue Depth';
	}

	if ($mdsData{req_active}{value} != $NO_SAMPLE) {
	  push @$ref1, 'lusmds.active';
	  push @$ref2, 'RPCs';
	  push @$ref3, $mdsData{req_active}{value};
	  push @$ref4, 'Lustre MDS RPC',;
	  push @$ref5, 'Active Requests';
	}

	if ($mdsData{req_timeout}{value} != $NO_SAMPLE) {
	  push @$ref1, 'lusmds.timeout';
	  push @$ref2, 'sec';
	  push @$ref3, $mdsData{req_timeout}{value};
	  push @$ref4, 'Lustre MDS RPC';
	  push @$ref5, 'Request Timeout';
	}

	if ($mdsData{reqbuf_avail}{value} != $NO_SAMPLE) {
	  push @$ref1, 'lusmds.buffers';
	  push @$ref2, 'RPC buffers';
	  push @$ref3, $mdsData{reqbuf_avail}{value};
	  push @$ref4, 'Lustre MDS RPC';
	  push @$ref5, 'Available Buffers';
	}

        my $getattrPlus = $mdsData{getattr}{value} +
          $mdsData{getattr_lock}{value} + 
          $mdsData{getxattr}{value};
        my $setattrPlus = $mdsData{reint_setattr}{value} +
	  $mdsData{setattr}{value} +
          $mdsData{setxattr}{value};

        push @$ref1, 'lusmds.gattrP';
        push @$ref2, 'ops/sec';
        push @$ref3, $getattrPlus / $intSecs;
        push @$ref4, 'Lustre MDS';
        push @$ref5, 'Get Attributes';
        
        push @$ref1, 'lusmds.sattrP';
        push @$ref2, 'ops/sec';
        push @$ref3, $setattrPlus / $intSecs;
        push @$ref4, 'Lustre MDS';
        push @$ref5, 'Set Attributes';

        push @$ref1, 'lusmds.sync';
        push @$ref2, 'ops/sec';
        push @$ref3, $mdsData{sync}{value} / $intSecs;
        push @$ref4, 'Lustre MDS';
        push @$ref5, 'File Syncs';

        my $delete = $mdsData{reint_unlink}{value} +
	  $mdsData{unlink}{value} +
	  $mdsData{rmdir}{value};

        push @$ref1, 'lusmds.unlink';
        push @$ref2, 'ops/sec', 
        push @$ref3, $delete / $intSecs;
        push @$ref4, 'Lustre MDS';
        push @$ref5, 'File/Dir Deletes';

        my $create = $mdsData{file_create}{value} + 
	  $mdsData{mkdir}{value} +
          $mdsData{reint_create}{value};

        push @$ref1, 'lusmds.create';
        push @$ref2, 'ops/sec';
        push @$ref3, $create / $intSecs;
        push @$ref4, 'Lustre MDS';
        push @$ref5, 'File/Dir Creates';

        push @$ref1, 'lusmds.open';
        push @$ref2, 'ops/sec',
        push @$ref3, $mdsData{open}{value} / $intSecs,;
        push @$ref4, 'Lustre MDS';
        push @$ref5, 'File Opens';

        push @$ref1, 'lusmds.close';
        push @$ref2, 'ops/sec';
        push @$ref3, $mdsData{close}{value} / $intSecs;
        push @$ref4, 'Lustre MDS';
        push @$ref5, 'File Closes';

        if ($lustOpts =~ /C/) {
          foreach my $clientName (@clientNames) {
            my $client = $mdsClientData{$clientName};

            push @$ref1, "$clientName.gattrP";
            push @$ref2, 'ops/sec';
            push @$ref3, $client->{getattr}{value} / $intSecs;
            push @$ref4, 'Lustre Client Get Inode Attribute';
            push @$ref5, "Get Attr. - $clientName";

            push @$ref1, "$clientName.sattrP";
            push @$ref2, 'ops/sec';
            push @$ref3, $client->{setattr}{value} / $intSecs;
            push @$ref4, 'Lustre Client Set Inode Attribute';
            push @$ref5, "Set Attr. - $clientName";

            push @$ref1, "$clientName.unlink";
            push @$ref2, "ops/sec";
            push @$ref3, $client->{unlink}{value} / $intSecs;
            push @$ref4, 'Lustre Client File Delete';
            push @$ref5, "File Deletes - $clientName";

            push @$ref1, "$clientName.create";
            push @$ref2, 'creates/sec';
            push @$ref3, $client->{file_create}{value} / $intSecs;
            push @$ref4, 'Lustre Client File Create';
            push @$ref5, "File Creates - $clientName";

            push @$ref1, "$clientName.open";
            push @$ref2, 'opens/sec';
            push @$ref3, $client->{open}{value} / $intSecs;
            push @$ref4, 'Lustre Client File Open';
            push @$ref5, "File Opens - $clientName";

            push @$ref1, "$clientName.close";
            push @$ref2, 'closes/sec';
            push @$ref3, $client->{close}{value} / $intSecs;
            push @$ref4, 'Lustre Client File Close';
            push @$ref5, "File Closes - $clientName";
          }
        }
      }
    }
  } elsif ($type eq 'l') {
    if ($lustOpts =~ /s/) {
      if ($mdsFlag) {
        my $getattrPlus = 
          $mdsData{getattr}{value} +
          $mdsData{getattr_lock}{value} + 
          $mdsData{getxattr}{value};
        my $setattrPlus = $mdsData{reint_setattr}{value} +
	  $mdsData{setattr}{value} +
          $mdsData{setxattr}{value};
        my $create = $mdsData{file_create}{value} + 
	  $mdsData{mkdir}{value} +
          $mdsData{reint_create}{value};
        my $delete = $mdsData{reint_unlink}{value} +
	  $mdsData{unlink}{value} +
	  $mdsData{rmdir}{value};
        
        push @$ref1, 'lusmds.gattrP';
        push @$ref2, $getattrPlus / $intSecs;
        
        push @$ref1, 'lusmds.sattrP';
        push @$ref2, $setattrPlus / $intSecs;
        
        push @$ref1, 'lusmds.sync';
        push @$ref2, $mdsData{sync}{value} / $intSecs;
        
        push @$ref1, 'lusmds.unlink';
        push @$ref2, $delete / $intSecs;
        
        push @$ref1, 'lusmds.create';
        push @$ref2, $create / $intSecs;
      }
    }
  } elsif ($type eq 's') {
    if ($lustOpts =~ /s/) {
      my $pad = $XCFlag ? '  ' : '';
      
      if ($mdsFlag) {
        my $getattrPlus = 
          $mdsData{getattr}{last} +
          $mdsData{getattr_lock}{last} + 
          $mdsData{getxattr}{last};
        my $setattrPlus = $mdsData{reint_setattr}{last} +
	  $mdsData{setattr}{last} +
          $mdsData{setxattr}{last};
        my $create = $mdsData{file_create}{last} +
	  $mdsData{mkdir}{last} +
          $mdsData{reint_create}{last};
        my $delete = $mdsData{reint_unlink}{last} +
	  $mdsData{unlink}{last} +
	  $mdsData{rmdir}{last};
        
        $$ref1 .= 
          "$pad(lusmds (getattrP $getattrPlus) (setattrP $setattrPlus) ";
        $$ref1 .= "(sync $mdsData{sync}{last}) ('unlink' $delete) ";
        $$ref1 .= "(create $create))\n";
      }
    }
  }
}

1;
