use strict;
package LustreCommon;

use constant NO_SAMPLE => -12341234;

sub lustreGetRpcStats {
  my $proc = shift;
  my $tag = shift;
  
  if (!open PROC, "<$proc") {
    return(0);
  }
  while (my $line = <PROC>) {
    if (($line =~ /req_waittime /) ||
	($line =~ /req_qdepth /) ||
	($line =~ /req_active /) ||
	($line =~ /req_timeout /) ||
	($line =~ /reqbuf_avail /)) {
      main::record(2, "$tag $line" );
    }
  }
  close PROC;
  return(1);
}

sub transLustreUUID {
  my $name = shift;
  my $Host = shift;
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

sub sumMetricValStr {
  my $val = shift;
  my $width = shift;

  return ($val == NO_SAMPLE) ? 
      sprintf("%".$width."s", " ") : 
      sprintf("%".$width."d", $val);
}

sub updateSumMetric {
  my $metric = shift;
  my $cumulCount = shift;
  my $sum = shift;

  if (defined $metric->{lastCumulCount} && defined $metric->{lastSum}) {
    if ($cumulCount == $metric->{lastCumulCount}) {
      $metric->{value} = LustreCommon::NO_SAMPLE;
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

sub delta {
  my $current = shift;
  my $last = shift;

  return (defined $last && ($current > $last)) ? $current - $last : 0;
}

1;
