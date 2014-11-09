# copyright, 2014 Terascala, Inc. All rights reserved
#
# lustreSingleton may be copied only under the terms of either the 
# Artistic License or the GNU General Public License

package LustreSingleton;


sub new {
	my $class = shift;
	$singleton = $singleton || bless {}, $class;
}

sub setDebug {
	$this = shift;
	$this->{_debug} = shift;
}

sub getVersion {
	$this = shift;
	# the null if block, a self-documenting
	if( $this->{_version} )
	{
		# version already retrieved, do nothing
		if($this->{_debug}){ print "version already retrieved, do nothing\n"; }
	}
	else
	{
    my $version_filename = '/proc/fs/lustre/version';
    if (-e $version_filename) {
      if($this->{_debug}){ print "retrieving version\n"; }
      open(FF,"<$version_filename");
      my $version_line = <FF>;
      close(FF);
      chomp($version_line);
      my @version = split(" ",$version_line);
      $this->{_version} = $version[1];	
    }
	}
	return $this->{_version};
}

1;
