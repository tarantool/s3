package = 's3'
version = 'scm-1'

source  = {
    url    = 'git://github.com/tarantool/s3.git';
    branch = 'master';
}

description = {
    summary  = "Tarantool backup in amazon s3";
    detailed = [[
        Tarantool backup in amazon s3
    ]];
    homepage = 'https://github.com/tarantool/s3.git';
    license  = 'BSD';
    maintainer = "Andrey Drozdov <andrey@tarantool.org>";
}

dependencies = {
    'lua >= 5.1';
}

external_dependencies = {
    TARANTOOL = {
        header = 'tarantool/module.h'
    };
}

build = {
    type = 'builtin',

    modules = {
        ['s3'] = 's3/init.lua';
        ['s3.cfunctions'] = {
            incdirs = {
                '$(TARANTOOL_INCDIR)';
            };
            sources = 's3/cfunctions.c';
        }
    }
}
-- vim: syntax=lua ts=4 sts=4 sw=4 et
