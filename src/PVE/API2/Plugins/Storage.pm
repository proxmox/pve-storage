package PVE::API2::Plugins::Storage;

use v5.36;

use PVE::RESTHandler;

use PVE::API2::Plugins::Storage::Plugin;

use base qw(PVE::RESTHandler);

__PACKAGE__->register_method({
    subclass => 'PVE::API2::Plugins::Storage::Plugin',
    path => 'plugin',
});

# plugins/storage

__PACKAGE__->register_method({
    name => 'index',
    path => '',
    method => 'GET',
    description => 'Directory index.',
    permissions => {
        user => 'all',
    },
    parameters => {
        additionalProperties => 0,
        properties => {},
    },
    returns => {
        type => 'array',
        items => {
            type => "object",
            additionalProperties => 0,
            properties => {
                subdir => {
                    type => 'string',
                },
            },
        },
        links => [{ rel => 'child', href => "{subdir}" }],
    },
    code => sub($param) {
        my $result = [];

        my $attrs = __PACKAGE__->method_attributes();

        for my $info ($attrs->@*) {
            next if !$info->{subclass};

            push $result->@*, { subdir => $info->{match_re}->[0] };
        }

        return $result;
    },
});

1;
