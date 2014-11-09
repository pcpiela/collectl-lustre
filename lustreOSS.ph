# copyright, 2014 Terascala, Inc. All rights reserved
#
# lustreOSS may be copied only under the terms of either the Artistic License
# or the GNU General Public License

# Lustre OSS Data Collector

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
my $printMsg = $debug & 16384;

my @clientNames = (); # Ordered list of OSS clients
my %clientNamesHash = ();
my $clientNamesStr = ''; # Concatenated string of client names
my $numClients = 0; # Number of clients accessing this OSS

my $numOst = 0; # Number of object storage targets
my $ossFlag = 0;
my $reportOssFlag = 0;
my @ostNames = (); # Ordered list of OST names
my $ostNamesStr = ''; # Concatenated string of OST names
my %ostSubdirs = ();
my %ostClientRead = ();
my %ostClientWrite = ();
my $ostWidth = 0;
my %ostData = (); # OSS performance metrics indexed by OST name

my $lustreWaitDur = 0;
my $lustreWaitDurLast = 0;
my $lustreQDepth = 0;
my $lustreQDepthLast = 0;
my $lustreActive = 0;
my $lustreActiveLast = 0;
my $lustreTimeout = 0;
my $lustreTimeoutLast = 0;
my $lustreReqBufs = 0;
my $lustreReqBufsLast = 0;
my @lustreBufReadTot = [];
my @lustreBufWriteTot = [];
my @lustreDiskIoSizeReadTot = [];
my @lustreDiskIoSizeWriteTot = [];

my $lustreReadKBytesTot = 0;
my $lustreReadKBytesTOT = 0;
my $lustreReadOpsTot = 0;
my $lustreReadOpsTOT = 0;
my $lustreWriteKBytesTot = 0;
my $lustreWriteKBytesTOT = 0;
my $lustreWriteOpsTot = 0;
my $lustreWriteOpsTOT = 0;

my $limLusKBS = 100;

# Global to count how many buckets there are for brw_stats
my @brwBuckets = [];
my $numBrwBuckets = scalar(@brwBuckets);

# Global to count how many buckets there are for brw disk i/o size stats
my @brwDiskIoSizeBuckets = [];
my $numBrwDiskIoSizeBuckets = scalar(@brwDiskIoSizeBuckets);

if ($printMsg) {
  if (defined($lustre_version)) {
    logmsg('I', "Lustre version $lustre_version");
  } else {
    logmsg('I', "Lustre version currently unknown");
  }
}

=pod
sub transLustreUUID {
  my $name = shift;
  my $hostRoot;
  
  # This handles names like OST_Lustre9_2_UUID or OST_Lustre9_UUID
  # changing them to just 0,9 or ost123.
  chomp $name;
  $hostRoot = $Host;
  $hostRoot =~ s/\d+$//;
  $name =~ s/OST_$hostRoot\d+//;
  $name =~ s/_UUID//;
  $name =~ s/_//;
  $name = 0 if $name eq '';
  
  return($name);
}
=cut
sub createOst {
  my $ost = {read => {%{$METRIC}},
             readKB => {%{$METRIC}},
             write => {%{$METRIC}},
             writeKB => {%{$METRIC}},
             rpc_read => [],
             rpc_write => [],
             disk_iosize_read => [],
             disk_iosize_write => []};

  for (my $j = 0; $j < $numBrwBuckets; $j++) {
    $ost->{rpc_read}[$j] = {%{$METRIC}};
    $ost->{rpc_write}[$j] = {%{$METRIC}};
  }
      
  for (my $j = 0; $j < $numBrwDiskIoSizeBuckets; $j++) {
    $ost->{disk_iosize_read}[$j] = {%{$METRIC}};
    $ost->{disk_iosize_write}[$j] = {%{$METRIC}};
  }

  return $ost;
}

sub lustreCheckOss { 
  $lustre_version = $lustre_singleton->getVersion()
    if (!defined($lustre_version));

  return 0 if (!defined($lustre_version));

  error("Lustre versions earlier than 1.8.0 are not currently supported")
    if ($lustre_version lt '1.8.0');

  # if this wasn't an OSS and still isn't, nothing has changed.
  return 0 if ($numOst == 0) && !-e "/proc/fs/lustre/obdfilter";

  undef %ostSubdirs;

  my @saveOstNames = @ostNames;
  my $saveOstNamesStr = $ostNamesStr;

  $ossFlag = $reportOssFlag = 0;

  $numOst = 0;
  @ostNames = ();

  my %ostNamesHash = ();
  my $subdir;
  my @ostFiles = glob("/proc/fs/lustre/obdfilter/*/stats");
  foreach my $file (@ostFiles) {
    $file =~ m[/proc/fs/lustre/obdfilter/(.*)/stats];
    $subdir = $1;
    
    my $uuid = cat("/proc/fs/lustre/obdfilter/$subdir/uuid");
    my $ostName = transLustreUUID($uuid);
    $ostWidth = length($ostName) if $ostWidth < length($ostName);
    $ossFlag = $reportOssFlag = 1;

    $numOst++;
    $ostNamesHash{$ostName} = $ostName;
    push(@ostNames, $ostName);
    $ostSubdirs{$ostName} = $subdir;

    if (!exists($ostData{$ostName})) {
      logmsg("I", "Adding OSS OST $ostName") if $printMsg;

      $ostData{$ostName} = createOst();
      
      $ostClientRead{$ostName} = {};
      $ostClientWrite{$ostName} = {};
      
      for my $client (@clientNames) {
        $ostClientRead{$ostName}{$client} = {%{$METRIC}};
        $ostClientWrite{$ostName}{$client} = {%{$METRIC}};
      }
    }
  }
  $ostWidth = 3 if $ostWidth < 3;
  $ostNamesStr = join(' ', @ostNames);

  if ($ostNamesStr ne $saveOstNamesStr) {
    # Delete OSTs that no longer exist
    foreach my $ostName (@saveOstNames) {
      if (!exists($ostNamesHash{$ostName})) {
        logmsg("I", "Removing OSS OST $ostName") if $printMsg;

        delete $ostData{$ostName};
        delete $ostClientRead{$ostName};
        delete $ostClientWrite{$ostName};
      }
    }
  }
  
  if (defined($lustOpts) && $lustOpts =~ /C/) {
    my @saveClientNames = @clientNames;
    my %saveClientNamesHash = %clientNamesHash;

    %clientNamesHash = ();
    @clientNames = ();
    $numClients = 0;

    if (defined($subdir)) {
      my @clientDirs = glob("/proc/fs/lustre/obdfilter/$subdir/exports/*");
      foreach my $client (@clientDirs) {
        next if $client =~ /clear/;
      
        $client = basename($client);
        $clientNamesHash{$client} = $client;
        push(@clientNames, $client);
        $numClients++;

        if (!exists($saveClientNamesHash{$client})) {
          logmsg("I", "Adding OSS client $client") if $printMsg;

          for my $ostName (@ostNames) {
            $ostClientRead{$ostName}{$client} = {%{$METRIC}};
            $ostClientWrite{$ostName}{$client} = {%{$METRIC}};
          }
        }
      }
    }
    $clientNamesStr = join(' ', @clientNames);

    if ($printMsg) {
      print "numClients: $numClients\n" .
        "Lustre clients: $clientNamesStr\n";
    }

    # Remove client entries that no longer exist
    foreach my $client (@saveClientNames) {
      if (!exists($clientNamesHash{$client})) {
        logmsg("I", "Removing OSS client $client") if $printMsg;

        foreach my $ostName (keys %ostClientRead) {
          delete $ostClientRead{$ostName}{$client};
        }

        foreach my $ostName (keys %ostClientWrite) {
          delete $ostClientWrite{$ostName}{$client};
        }
      }
    }
  }
  
  # Change info is important even when not logging except during initialization
  if ($ostNamesStr ne $saveOstNamesStr) {
    my $comment = ($filename eq '') ? '#' : '';
    my $text = "Lustre OSS OSTs Changed -- Old: $saveOstNamesStr  New: $ostNamesStr";
    logmsg('W', "${comment}$text") if !$firstPass;
    print "$text\n" if $firstPass && $printMsg;
  }
  return ($ostNamesStr ne $saveOstNamesStr) ? 1 : 0;
}

sub lustreGetRpcStats {
  my ($proc, $tag) = @_;
  if (!open PROC, "<$proc") {
    return(0);
  }
  while (my $line = <PROC>) {
    if (($line =~ /req_waittime/) ||
        ($line =~ /req_qdepth/) ||
        ($line =~ /req_active/) ||
        ($line =~ /req_timeout/) ||
        ($line =~ /reqbuf_avail/)) {
      record(2, "$tag $line" ); 
    }
  }
  close PROC;
  return(1);
}

sub lustreGetOstStats {
  my ($ostName) = @_;
  my $tag = "OST_$ostName";

  my $proc = "/proc/fs/lustre/obdfilter/$ostSubdirs{$ostName}/stats";
  return(0) if (!open PROC, "<$proc");

  while (my $line = <PROC>) {
    if (($line =~ /^read/) || ($line =~ /^write/)) { 
      record(2, "$tag $line"); 
    }
  }
  close PROC;
  return(1);
}

sub lustreGetOstRpcSizeStats {
  my ($ostName) = @_;
  my $tag = "OST-b_$ostName";

  my $proc = "/proc/fs/lustre/obdfilter/$ostSubdirs{$ostName}/brw_stats";
  return(0) if (!open PROC, "<$proc");

  # Skip to beginning of rpc data
  for (my $i = 0; $i < 4; $i++) {
    <PROC>;
  }

  my $index = 0;
  while (my $line = <PROC>) {
    last if $line =~ /^\s*$/;
    record(2, "$tag:$index $line");
    $index++;
  }
  close PROC;
  return(1);
}

sub lustreGetOstDiskIoSizeStats {
  my ($ostName) = @_;
  my $tag = "OST-c_$ostName";

  my $proc = "/proc/fs/lustre/obdfilter/$ostSubdirs{$ostName}/brw_stats";
  return(0) if (!open PROC, "<$proc");

  # Skip to beginning of disk io data
  while (my $line = <PROC>) {
    last if (index($line, "disk I/O size") == 0);
  }

  my $index = 0;
  while (my $line = <PROC>) {
    last if $line =~ /^\s*$/;
    record(2, "$tag:$index $line");
    $index++;
    last if $index >= $numBrwDiskIoSizeBuckets;
  }
  close PROC;
  return(1);
}

sub lustreGetOstClientStats {
  my ($ostName, $client) = @_;
  my $tag = "OST_$ostName";

  my $proc = "/proc/fs/lustre/obdfilter/$ostSubdirs{$ostName}/exports/$client/stats";
  return(0) if (!open PROC, "<$proc");

  while (my $line = <PROC>) {
    if (($line =~ /^read_bytes/) || ($line =~ /^write_bytes/)) { 
      record(2, "$tag LCL_$client" . "_$line"); 
    }
  }
  close PROC;
  return(1);
}

sub lustreGetOssStats {
  lustreGetRpcStats("/proc/fs/lustre/ost/OSS/ost/stats", "OST_RPC");

  foreach my $ostName (@ostNames) {
    lustreGetOstStats($ostName);

    if ($lustOpts =~ /B/) {
      lustreGetOstRpcSizeStats($ostName) ;
      lustreGetOstDiskIoSizeStats($ostName);
    }
        
    foreach my $client (@clientNames) {
      lustreGetOstClientStats($ostName, $client);
    }
  }
  return(1);
}

sub lustreOSSInit {
  my $impOptsref = shift;
  my $impKeyref = shift;

  error("You must remove the -sl or -sL option to use this plugin")
    if ($subsys =~ /l/i);

  $lustOpts = ${$impOptsref};
  error('Valid lustre options are: s d B C') 
    if defined($lustOpts) && $lustOpts !~ /^[sdBC]*$/;

  $lustOpts = 's' if !defined($lustOpts);
  ${impOptsref} = $lustOpts;

  error("Lustre versions earlier than 1.8.0 are not currently supported")
    if (defined($lustre_version) && ($lustre_version lt '1.8.0'));

  print "lustreOSSInit: options: $lustOpts\n" if $printMsg;

  ${$impKeyref} = 'OST';

  @brwBuckets = (1,2,4,8,16,32,64,128,256);
  $numBrwBuckets = scalar(@brwBuckets);

  @brwDiskIoSizeBuckets = 
      ('4K', '8K', '16K', '32K', '64K', '128K', '256K', '512K', '1M');
  $numBrwDiskIoSizeBuckets = scalar(@brwDiskIoSizeBuckets);

  lustreCheckOss();

  $verboseFlag = 1 if $lustOpts =~ /[B]/;

  $lustOptsOnly = $lustOpts;
  $lustOptsOnly =~ s/[ds]//;

  return(1);
}

sub lustreOSSUpdateHeader {
  my $lineref = shift;

  ${$lineref} .= 
    "# Lustre OSS Data Collector: Version 1.0, Lustre version: $lustre_version\n";
}

sub lustreOSSGetData {
  lustreGetOssStats() if ($numOst > 0);
}

sub lustreOSSInitInterval {
  # Check to see if any services changed and if they did, we may need
  # a new logfile as well.
  newLog($filename, "", "", "", "", "") if lustreCheckOss() && $filename ne '';

  $sameColsFlag = 0 if length($lustOptsOnly) > 1;

  $lustreReadOpsTot = 0;
  $lustreReadKBytesTot = 0;
  $lustreWriteOpsTot = 0;
  $lustreWriteKBytesTot = 0;
  for (my $i = 0; $i < $numBrwBuckets; $i++) {
    $lustreBufReadTot[$i] = 0;
    $lustreBufWriteTot[$i] = 0;
  }
  for (my $i = 0; $i < $numBrwDiskIoSizeBuckets; $i++) {
    $lustreDiskIoSizeReadTot[$i] = 0;
    $lustreDiskIoSizeWriteTot[$i] = 0;
  }
}

sub delta {
  my $current = shift;
  my $last = shift;

  return ($current > $last) ? $current - $last : 0;
}

sub lustreOSSAnalyze {
  my $type = shift;
  my $dataref = shift;
  my $data = ${$dataref};

  logmsg('I', "lustreAnalyze: type: $type, data: $data") if $printMsg;

  if ($type =~ /OST_RPC/) {
    my ($metric, $value) = (split(/\s+/, $data))[0, 6];

    if ($metric =~ /^req_waittime/) {
      $lustreWaitDur = delta($value, $lustreWaitDurLast);
      $lustreWaitDurLast = $value;
    } elsif ($metric =~ /^req_qdepth/) {
      $lustreQDepth = delta($value, $lustreQDepthLast);
      $lustreQDepthLast = $value;
    } elsif ($metric =~ /^req_active/) {
      $lustreActive = delta($value, $lustreActiveLast);
      $lustreActiveLast = $value;
    } elsif ($metric =~ /^req_timeout/) {
      $lustreTimeout = delta($value, $lustreTimeoutLast);
      $lustreTimeoutLast = $value;
    } elsif ($metric =~ /^reqbuf_avail/) {
      $lustreReqBufs = delta($value, $lustreReqBufsLast);
      $lustreReqBufsLast = $value;
    }
  } elsif ($type =~ /OST_(.*)/) {
    my $ostName = $1;
    my ($metric, $ops, $bytes) = (split(/\s+/, $data))[0, 1, 6];

    $bytes = 0 if $ops == 0;
    if ($metric =~ /^read/) {
      my $attr = $ostData{$ostName}{read};
      my $val = delta($ops, $attr->{last});
      $attr->{value} = $val;
      $attr->{last} = $ops;
      $lustreReadOpsTot += $val;

      $attr = $ostData{$ostName}{readKB};
      my $KBytes = $bytes / $OneKB;
      $val = delta($KBytes, $attr->{last});
      $attr->{value} = $val;
      $attr->{last} = $KBytes;
      $lustreReadKBytesTot += $val;
    } elsif ($metric =~ /^write/) {
      my $attr = ${ostData}{$ostName}{write};
      my $val = delta($ops, $attr->{last});
      $attr->{value} = $val;
      $attr->{last} = $ops;
      $lustreWriteOpsTot += $val;

      $attr = $ostData{$ostName}{writeKB};
      my $KBytes = $bytes / $OneKB;
      $val = delta($KBytes, $attr->{last});
      $attr->{value} = $val;
      $attr->{last} = $KBytes;
      $lustreWriteKBytesTot += $val;
    } elsif ($metric =~ /^LCL/) {
      my $client;
      my @tmpNames = split("_", $metric);
      foreach (@tmpNames) {
        if ($_ =~/@/) {
          $client = $_;
          last;
        }
      }
      
      if (defined($bytes)) {
        my $ostAttr;
        if ($metric =~ /read_bytes/) {
          $ostAttr = $ostClientRead{$ostName};
        } elsif ($metric =~ /write_bytes/) {
          $ostAttr = $ostClientWrite{$ostName};
        }
        if (defined($ostAttr)) {
          if (!exists($ostAttr->{$client})) {
            $ostAttr->{$client} = {'value' => 0, 'last' => $bytes};
          } else {
            my $attr = ${$ostAttr}{$client};
            $attr->{value} = delta($bytes, $attr->{last});
            $attr->{last} = $bytes;
          }
        }
      }
    }
  } elsif ($type =~ /OST-b_(.*):(\d+)/) {   
    my $ostName = $1;

    my $size = (split(/:/, $data))[0];
    my $bufNum = 0;
    foreach my $i (@brwBuckets) { 
      last if ($size == $i);
      $bufNum = $bufNum + 1;
    }
    
    my ($reads, $writes) = (split(/\s+/, $data))[1,5];

    my $attr = $ostData{$ostName}{rpc_read}[$bufNum];
    my $val = delta($reads, $attr->{last});
    $attr->{value} = $val;
    $attr->{last} = $reads;
    $lustreBufReadTot[$bufNum] += $val;

    $attr = $ostData{$ostName}{rpc_write}[$bufNum];
    $val = delta($writes, $attr->{last});
    $attr->{value} = $val;
    $attr->{last} = $writes;
    $lustreBufWriteTot[$bufNum] += $val;
  } elsif ($type =~ /OST-c_(.*):(\d+)/) {
    my $ostName = $1;

    my $size = (split(/:/, $data))[0];
    my $bufNum = 0;
    foreach my $i (@brwDiskIoSizeBuckets) { 
      last if ($size eq $i);
      $bufNum = $bufNum + 1;
    }

    my ($reads, $writes) = (split(/\s+/, $data))[1,5];

    my $attr = $ostData{$ostName}{disk_iosize_read}[$bufNum];
    my $val = delta($reads, $attr->{last});
    $attr->{value} = $val;
    $attr->{last} = $reads;
    $lustreDiskIoSizeReadTot[$bufNum] += $val;

    $attr = $ostData{$ostName}{disk_iosize_write}[$bufNum];
    $val = delta($writes - $attr->{last});
    $attr->{value} = $val;
    $attr->{last} = $writes;
    $lustreDiskIoSizeWriteTot[$bufNum] += $val;
  }
}

# This and the 'print' routines should be self explanitory as they pretty much simply
# return a string in the appropriate format for collectl to dispose of.
sub lustreOSSPrintBrief {
  my $type = shift;
  my $lineref = shift;
  
  if ($type == 1) { # header line 1
    ${$lineref} .= "<---------Lustre OST--------->" 
      if $lustOpts =~ /s/ && $reportOssFlag && !$ioSizeFlag;
    ${$lineref} .= "<--------------Lustre OST-------------->"
      if $lustOpts =~ /s/ && $reportOssFlag &&  $ioSizeFlag;
  } elsif ($type == 2) { # header line 2
    ${$lineref} .= " KBRead  Reads  KBWrit Writes " 
      if $lustOpts =~ /s/ && $reportOssFlag && !$ioSizeFlag;
    ${$lineref} .= " KBRead  Reads Size  KBWrit Writes Size " 
      if $lustOpts =~ /s/ && $reportOssFlag && $ioSizeFlag;
  } elsif ($type == 3) { # data
    # OSS
    if ($lustOpts =~ /s/ && $reportOssFlag) {
      if (!$ioSizeFlag) {
        ${$lineref} .= 
          sprintf("%7s %6s %7s %6s ",
                  cvt($lustreReadKBytesTot / $intSecs, 7, 0, 1),
                  cvt($lustreReadOpsTot / $intSecs, 6),
                  cvt($lustreWriteKBytesTot / $intSecs,7, 0, 1),
                  cvt($lustreWriteOpsTot / $intSecs, 6));
      } else {
        ${$lineref} .= 
          sprintf("%7s %6s %4s %7s %6s %4s ",
                  cvt($lustreReadKBytesTot / $intSecs, 7, 0, 1),
                  cvt($lustreReadOpsTot / $intSecs, 6),
                  $lustreReadOpsTot ? 
                  cvt($lustreReadKBytesTot / $lustreReadOpsTot, 4, 0, 1) : 0,
                  cvt($lustreWriteKBytesTot / $intSecs, 7, 0, 1),
                  cvt($lustreWriteOpsTot / $intSecs, 6),
                  $lustreWriteOpsTot ? 
                  cvt($lustreWriteKBytesTot / $lustreWriteOpsTot, 4, 0, 1) : 0);
      }
    }
  } elsif ($type == 4) { # reset 'total' counters
    $lustreReadKBytesTOT = 0;
    $lustreReadOpsTOT = 0;
    $lustreWriteKBytesTOT = 0;
    $lustreWriteOpsTOT = 0;
  } elsif ($type == 5) { # increment 'total' counters
    if ($numOst) {
      $lustreReadKBytesTOT += $lustreReadKBytesTot;
      $lustreReadOpsTOT += $lustreReadOpsTot;
      $lustreWriteKBytesTOT += $lustreWriteKBytesTot;
      $lustreWriteOpsTOT += $lustreWriteOpsTot;
    }
  } elsif ($type == 6) { # print 'total' counters
    # Since this never goes over a socket we can just do a simple print.
    if ($lustOpts =~ /s/ && $reportOssFlag) {
      if (!$ioSizeFlag) {
        printf "%7s %6s %7s %6s ",
        cvt($lustreReadKBytesTOT / $totSecs, 7, 0, 1),
        cvt($lustreReadOpsTOT / $totSecs, 6),
        cvt($lustreWriteKBytesTOT / $totSecs, 7, 0, 1),
        cvt($lustreWriteOpsTOT / $totSecs, 6);
      } else {
        printf "%7s %6s %4s %7s %6s %4s ",
        cvt($lustreReadKBytesTOT / $totSecs, 7, 0, 1),
        cvt($lustreReadOpsTOT / $totSecs, 6),
        $lustreReadOpsTOT ? cvt($lustreReadKBytesTOT / $lustreReadOpsTOT, 
                                4, 0, 1) : 0,
        cvt($lustreWriteKBytesTOT / $totSecs, 7, 0, 1), 
        cvt($lustreWriteOpsTOT / $totSecs, 6),
        $lustreWriteOpsTOT ? cvt($lustreWriteKBytesTOT / $lustreWriteOpsTOT,
                                 4, 0, 1) : 0;
      }
    }
  }
}

sub lustreOSSPrintVerbose {
  my $printHeader = shift;
  my $homeFlag = shift;
  my $lineref = shift;

  # Note that last line of verbose data (if any) still sitting in $$lineref
  my $line = ${$lineref} = '';
  
  # This is the normal output for an OSS
  if ($lustOpts =~ /s/ && $reportOssFlag) {
    if ($printHeader) {
      my $line = '';
      $line .= "\n" if !$homeFlag;
      $line .= "# LUSTRE OST SUMMARY ($rate)\n";
      if ($lustOpts !~ /B/) {
        $line .= "#${miniDateTime}  KBRead   Reads  SizeKB  KBWrite  Writes  SizeKB\n";
      } else {
        $line .= "#${miniFiller}<----------------------reads-------------------------|";
        $line .= "-----------------------writes------------------------->\n";
        my $temp = '';
        foreach my $i (@brwBuckets) {
          $temp .= sprintf(" %3dP", $i); 
        }
        $line .= "#${miniDateTime}RdK  Rds$temp WrtK Wrts$temp\n";
      }
      ${$lineref} .= $line;
      exit if $showColFlag;
    }
    
    my $line = $datetime;
    if ($lustOpts !~ /B/) {
      $line .= 
        sprintf("  %7d  %6d  %6s  %7d  %6d  %6s",
                $lustreReadKBytesTot / $intSecs,
                $lustreReadOpsTot / $intSecs,
                $lustreReadOpsTot ? 
                cvt($lustreReadKBytesTot / $lustreReadOpsTot, 6, 0, 1) : 0,
                $lustreWriteKBytesTot / $intSecs,
                $lustreWriteOpsTot / $intSecs,
                $lustreWriteOpsTot ? 
                cvt($lustreWriteKBytesTot / $lustreWriteOpsTot, 6, 0, 1) : 0);
    } else {
      $line .= sprintf("%4s %4s",
                       cvt($lustreReadKBytesTot / $intSecs, 4, 0, 1),
                       cvt($lustreReadOpsTot / $intSecs));
      for (my $i = 0; $i < $numBrwBuckets; $i++) {
        $line .= sprintf(" %4s", cvt($lustreBufReadTot[$i] / $intSecs));
      }
      
      $line .= sprintf(" %4s %4s",
                       cvt($lustreWriteKBytesTot / $intSecs, 4, 0, 1),
                       cvt($lustreWriteOpsTot / $intSecs));
      for (my $i = 0; $i < $numBrwBuckets; $i++) {
        $line .= sprintf(" %4s", cvt($lustreBufWriteTot[$i] / $intSecs));
      }
    }
    $line .= "\n";
    ${$lineref} .= $line;
  }

  if ($lustOpts =~ /d/ && $reportOssFlag) {
    if ($printHeader) {
      # build ost header, and when no date/time make it even 1 char less.
      my $temp = "Ost". ' 'x$ostWidth;
      $temp = substr($temp, 0, $ostWidth);
      $temp = substr($temp, 0, $ostWidth - 2) . ' ' if $miniFiller eq '';
      
      # When doing dates/time shift first field over 1 to the left;
      my $fill1 = '';
      if ($miniFiller ne '') {
        $fill1 = substr($miniDateTime, 0, length($miniFiller) - 1);
      }
  
      my $line = '';
      $line .= "\n" if !$homeFlag;
      $line .= "# LUSTRE FILESYSTEM SINGLE OST STATISTICS ($rate)\n";
      if ($lustOpts !~ /B/) {
        $line .= "#$fill1$temp   KBRead   Reads  SizeKB    KBWrite  Writes  SizeKB\n";
      } else {
        my $temp2 = '';
        foreach my $i (@brwBuckets) {
          $temp2 .= sprintf(" %3dP", $i); 
        }
        $line .= "#$fill1$temp   RdK  Rds$temp2 WrtK Wrts$temp2\n";
      }
      ${$lineref} .= $line;
      exit if $showColFlag;
    }
    
    foreach my $ostName (@ostNames) {
      my $ost = $ostData{$ostName};
      
      # If exception processing in effect, make sure this entry qualities
      next if $options=~/x/ && 
        $ost->{readKB}{value} / $intSecs < $limLusKBS &&
        $ost->{writeKB}{value} / $intSecs < $limLusKBS;

      my $line = '';
      if ($lustOpts !~ /B/) {
        $line .= sprintf(
          "$datetime%-${ostWidth}s  %7d  %6d  %6d    %7d  %6d  %6d\n",
          $ostName,
          $ost->{readKB}{value} / $intSecs,
          $ost->{read}{value} / $intSecs,
          $ost->{read}{value} ? 
          $ost->{readKB}{value} / $ost->{read}{value} : 0,
          $ost->{writeKB}{value} / $intSecs,
          $ost->{write}{value} / $intSecs,
          $ost->{write}{value} ? 
          $ost->{writeKB}{value} / $ost->{write}{value} : 0);
      } else {
        $line .= sprintf(
          "$datetime%-${ostWidth}s  %4s %4s",
          $ostName, 
          cvt($ost->{readKB}{value} / $intSecs, 4, 0, 1),
          cvt($ost->{read}{value} / $intSecs));
        for (my $j = 0; $j < $numBrwBuckets; $j++) {
          $line .= sprintf(
            " %4s",
            cvt($ost->{rpc_read}[$j]{value} / $intSecs));
        }
        
        $line .= sprintf(
          " %4s %4s",
          cvt($ost->{writeKB}{value} / $intSecs, 4, 0, 1),
          cvt($ost->{write}{value} / $intSecs));
        for (my $j = 0; $j < $numBrwBuckets; $j++) {
          $line .= sprintf(
            " %4s",
            cvt($ost->{rpc_write}[$j]{value} / $intSecs));
        }
        $line .= "\n";
      }
      ${$lineref} .= $line;
    }
  }
}

# Just be sure to use $SEP in the right places.  A simple trick to make sure you've done it
# correctly is to generste a small plot file and load it into a speadsheet, making sure each
# column of data has a header and that they aling 1:1.
sub lustreOSSPrintPlot {
  my $type = shift;
  my $ref1 = shift;

  #    H e a d e r s

  # Summary
  if ($type == 1 && $lustOpts =~ /s/) {
    my $headers = '';

    if ($reportOssFlag) {
      # We always report basic I/O independent of what user selects with --lustopts
      $headers .= "[OST]Read${SEP}[OST]ReadKB${SEP}[OST]Write${SEP}[OST]WriteKB${SEP}";
      if ($lustOpts =~ /B/) {
        foreach my $i (@brwBuckets) {
          $headers .= "[OSTB]r${i}P${SEP}"; 
        }
        foreach my $i (@brwBuckets) {
          $headers .= "[OSTB]w${i}P${SEP}"; 
        }
      }
    }
    ${$ref1} .= $headers;
  }

  if ($type == 2 && $lustOpts =~ /d/) {
    if ($reportOssFlag) {
      # We always start with this section
      # BRW stats are optional, but if there group them together separately.

      my $ostHeaders = '';
      foreach my $ostName (@ostNames) {
        $ostHeaders .= "[OST:$ostName]Ost${SEP}[OST:$ostName]Read${SEP}[OST:$ostName]ReadKB${SEP}[OST:$ostName]Write${SEP}[OST:$ostName]WriteKB${SEP}";
      }

      if ($lustOpts =~ /B/) {
        foreach my $ostName (@ostNames) {
          foreach my $j (@brwBuckets) {
            $ostHeaders .= "[OSTB:$ostName]r$j${SEP}"; 
          }
          foreach my $j (@brwBuckets) {
            $ostHeaders .= "[OSTB:$ostName]w$j${SEP}"; 
          }
        }
      }
      ${$ref1} .= $ostHeaders;
    }
  }

  #    D a t a

  # Summary
  if ($type == 3 && $lustOpts =~ /s/) {
    my $plot = '';
    if ($reportOssFlag) {
      # We always do this...
      $plot .= sprintf("$SEP%$FS$SEP%$FS$SEP%$FS$SEP%$FS",
                       $lustreReadOpsTot / $intSecs,  
                       $lustreReadKBytesTot / $intSecs,
                       $lustreWriteOpsTot/$intSecs,
                       $lustreWriteKBytesTot/$intSecs);
      
      if ($lustOpts =~ /B/) {
        for (my $j = 0; $j < $numBrwBuckets; $j++) {
          $plot .= sprintf("$SEP%$FS", $lustreBufReadTot[$j] / $intSecs);
        }
        for (my $j = 0; $j < $numBrwBuckets; $j++) {
          $plot .= sprintf("$SEP%$FS", $lustreBufWriteTot[$j] / $intSecs);
        }
        for (my $j = 0; $j < $numBrwDiskIoSizeBuckets; $j++) {
          $plot .= sprintf("$SEP%$FS", $lustreDiskIoSizeReadTot[$j] / $intSecs);
        }
        for (my $j = 0; $j < $numBrwDiskIoSizeBuckets; $j++) {
          $plot .= sprintf("$SEP%$FS", $lustreDiskIoSizeWriteTot[$j] / $intSecs);
        }
      }
    }
    ${$ref1} .= $plot;
  }

  # Detail
  if ($type == 4 && $lustOpts =~ /d/) {
    if ($reportOssFlag) {
      # Basic I/O always there and grouped together
      my $ostPlot = '';
      foreach my $ostName (@ostNames) {
        my $ost = $ostData{$ostName};

        $ostPlot .= sprintf("$SEP%s$SEP%d$SEP%d$SEP%d$SEP%d",
                            $ostName,
                            $ost->{read}{value} / $intSecs, 
                            $ost->{readKB}{value} / $intSecs,
                            $ost->{write}{value} / $intSecs, 
                            $ost->{writeKB}{value} / $intSecs);
      }
      
      # These guys are optional and follow ALL the basic stuff     
      if ($lustOpts =~ /B/) {
        foreach my $ostName (@ostNames) {
          my $ost = $ostData{$ostName};

          for (my $j = 0; $j < $numBrwBuckets; $j++) {
            $ostPlot .= 
              sprintf("$SEP%d", 
                      $ost->{rpc_read}[$j]{value} / $intSecs); 
          }
          for (my $j = 0; $j < $numBrwBuckets; $j++) {
            $ostPlot .= 
              sprintf("$SEP%d", 
                      $ost->{rpc_write}[$j]{value} / $intSecs);
          }
          for (my $j = 0; $j < $numBrwDiskIoSizeBuckets; $j++) {
            $ostPlot .= 
              sprintf("$SEP%d", 
                      $ost->{disk_iosize_read}[$j]{value} / $intSecs);
          }
          for (my $j = 0; $j < $numBrwDiskIoSizeBuckets; $j++) {
            $ostPlot .= 
              sprintf("$SEP%d", 
                      $ost->{disk_iosize_write}[$j]{value} / $intSecs);
          }
        }
      }
      ${$ref1} .= $ostPlot;
    }
  }
}

sub lustreOSSPrintExport {
  my $type = shift;
  my $ref1 = shift;
  my $ref2 = shift;
  my $ref3 = shift;
  my $ref4 = shift;
  my $ref5 = shift;

  if ($type eq 'g') {
    if ($lustOpts =~ /s/) {
      if ($ossFlag) {
        push @$ref1, 'lusost.reads';
        push @$ref2, 'reads/sec';
        push @$ref3, $lustreReadOpsTot/$intSecs;
        push @$ref4, 'Lustre OST';
        push @$ref5, undef;
        
        push @$ref1, 'lusost.readkbs';
        push @$ref2, 'readkbs/sec';
        push @$ref3, $lustreReadKBytesTot / $intSecs;
        push @$ref4, 'Lustre OST';
        push @$ref5, undef;

        push @$ref1, 'lusost.writes';
        push @$ref2, 'writes/sec';
        push @$ref3, $lustreWriteOpsTot / $intSecs;
        push @$ref4, 'Lustre OST';
        push @$ref5, undef;

        push @$ref1, 'lusost.writekbs';
        push @$ref2, 'writekbs/sec';
        push @$ref3, $lustreWriteKBytesTot / $intSecs;
        push @$ref4, 'Lustre OST';
        push @$ref5, undef;
      }
    }

    if ($lustOpts =~ /d/) {
      if ($ossFlag) {
        push @$ref1, 'lusoss.waittime';
        push @$ref2, 'usec';
        push @$ref3, $lustreWaitDur;
        push @$ref4, 'Lustre OSS RPC';
        push @$ref5, 'Request Wait Time';
        
        push @$ref1, 'lusoss.qdepth';
        push @$ref2, 'queue depth';
        push @$ref3, $lustreQDepth;
        push @$ref4, 'Lustre OSS RPC';
        push @$ref5, 'Request Queue Depth';
        
        push @$ref1, 'lusoss.active';
        push @$ref2, 'RPCs';
        push @$ref3, $lustreActive;
        push @$ref4, 'Lustre OSS RPC';
        push @$ref5, 'Active Requests';
        
        push @$ref1, 'lusoss.timeouts';
        push @$ref2, 'RPC timeouts';
        push @$ref3, $lustreTimeout;
        push @$ref4, 'Lustre OSS RPC';
        push @$ref5, 'Request Timeouts';
        
        push @$ref1, 'lusoss.buffers';
        push @$ref2, 'RPC buffers';
        push @$ref3, $lustreReqBufs;
        push @$ref4, 'Lustre OSS RPC';
        push @$ref5, 'LNET Request Buffers';
        
        foreach my $ostName (@ostNames) {
          my $ost = $ostData{$ostName};
          
          push @$ref1, "lusost.reads.$ostName";
          push @$ref2, 'reads/sec';
          push @$ref3, $ost->{read}{value} / $intSecs;
          push @$ref4, 'Lustre OST Read';
          push @$ref5, "$ostName";
          
          push @$ref1, "lusost.readkbs.$ostName";
          push @$ref2, 'readkbs/sec';
          push @$ref3, $ost->{readKB}{value} / $intSecs;
          push @$ref4, 'Lustre OST Read Throughput';
          push @$ref5, "$ostName";

          push @$ref1, "lusost.writes.$ostName";
          push @$ref2, 'writes/sec';
          push @$ref3, $ost->{write}{value} / $intSecs;
          push @$ref4, 'Lustre OST Write';
          push @$ref5, "$ostName";
          
          push @$ref1, "lusost.writekbs.$ostName";
          push @$ref2, 'writekbs/sec';
          push @$ref3, $ost->{writeKB}{value} / $intSecs;
          push @$ref4, 'Lustre OST Write Throughput';
          push @$ref5, "$ostName";
          
          for (my $j = 0; $j < $numBrwBuckets; $j++) {
            my $bucketSize = sprintf("%04dk", $brwBuckets[$j] * 4);
            
            push @$ref1, "lustost.readHist.$ostName.$bucketSize";
            push @$ref2, 'reads/sec';
            push @$ref3, $ost->{rpc_read}[$j]{value} / $intSecs;
            push @$ref4, "Lustre RPC Size - Read";
            push @$ref5, "$ostName $bucketSize";
            
            push @$ref1, "lustost.writeHist.$ostName.$bucketSize";
            push @$ref2, 'writes/sec';
            push @$ref3, $ost->{rpc_write}[$j]{value} / $intSecs;
            push @$ref4, "Lustre RPC Size - Write";
            push @$ref5, "$ostName.$bucketSize";
          }
          
          for (my $j = 0; $j < $numBrwDiskIoSizeBuckets; $j++) {
            my $bucketSize = $brwDiskIoSizeBuckets[$j];
            if (index($bucketSize, "K") != -1) {
              $bucketSize =~ s/K//;
              $bucketSize *= 1;
            } elsif (index($bucketSize, "M") != -1) {
              $bucketSize =~ s/M//;
              $bucketSize *= 1024;
            }
            
            $bucketSize = sprintf("%04dk", $bucketSize);

            push @$ref1, "lustost.diskIoSizeRead.$ostName.$bucketSize";
            push @$ref2, 'reads/sec';
            push @$ref3, $ost->{disk_iosize_read}[$j]{value} / $intSecs;
            push @$ref4, "Lustre Disk IO Size - Read";
            push @$ref5, "$ostName $brwDiskIoSizeBuckets[$j]";

            push @$ref1, "lustost.diskIoSizeWrite.$ostName.$bucketSize";
            push @$ref2, 'writes/sec';
            push @$ref3, $ost->{disk_iosize_write}[$j]{value} / $intSecs;
            push @$ref4, "Lustre Disk IO Size - Write";
            push @$ref5, "$ostName $brwDiskIoSizeBuckets[$j]";
          }
        }
        
        if ($lustOpts =~ /C/) {
          foreach my $client (@clientNames) {
            my $cliRead = 0;
            my $cliWrite = 0;
            
            foreach my $ostName (@ostNames) {
              $cliRead += $ostClientRead{$ostName}{$client}{value} 
                if (exists($ostClientRead{$ostName}{$client}));
              $cliWrite += $ostClientWrite{$ostName}{$client}{value}
                if (exists($ostClientWrite{$ostName}{$client}));
            }
            
            push @$ref1, "$client.read"; 
            push @$ref2, "readkbs/sec";
            push @$ref3, $cliRead / ($intSecs * $OneKB);
            push @$ref4, 'Lustre Client Read Throughput';
            push @$ref5, "$client";
            
            push @$ref1, "$client.write";
            push @$ref2, "writekbs/sec";
            push @$ref3, $cliWrite / ($intSecs * $OneKB);
            push @$ref4, 'Lustre Client Write Throughput';
            push @$ref5, "$client";
          }
        }
      }
    }
  } elsif ($type eq 'l') {
    if ($lustOpts =~ /s/) {
      if ($ossFlag) {
        push @$ref1, "lusost.reads";
        push @$ref2, $lustreReadOpsTot / $intSecs;
        
        push @$ref1, "lusost.readkbs";
        push @$ref2, $lustreReadKBytesTot / $intSecs;
        
        push @$ref1, "lusost.writes";
        push @$ref2, $lustreWriteOpsTot / $intSecs;
        
        push @$ref1, "lusost.writekbs";
        push @$ref2, $lustreWriteKBytesTot / $intSecs;
      }
    }
  } elsif ($type eq 's') {
    if ($lustOpts =~ /s/) {
      my $pad = $XCFlag ? '  ' : '';

      if ($ossFlag) {
        my ($reads, $readKBs, $writes, $writeKBs) = (0, 0, 0, 0);
        foreach my $ostName (@ostNames) {
          my $ost = $ostData{$ostName};

          $reads += $ost->{read}{last};
          $readKBs += $ost->{readKB}{last};
          $writes += $ost->{write}{last};
          $writeKBs += $ost->{writeKB}{last};
        }
        $$ref1 .= "$pad(lusoss (reads $reads) (readkbs $readKBs) (writes $writes) (writekbs $writeKBs))\n";
      }
    }
  }
}

1;
