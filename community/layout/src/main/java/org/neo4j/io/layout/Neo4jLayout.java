/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.io.layout;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileUtils;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public final class Neo4jLayout
{
    private static final String STORE_LOCK_FILENAME = "store_lock";
    private static final String SERVER_ID_FILENAME = "server_id";

    private final Path homeDirectory;
    private final Path dataDirectory;
    private final Path databasesRootDirectory;
    private final Path txLogsRootDirectory;
    private final Path scriptRootDirectory;

    public static Neo4jLayout of( Path homeDirectory )
    {
        return of( Config.defaults( GraphDatabaseSettings.neo4j_home, FileUtils.getCanonicalFile( homeDirectory ).toAbsolutePath() ) );
    }

    public static Neo4jLayout of( Config config )
    {
        var homeDirectory = config.get( GraphDatabaseSettings.neo4j_home );
        var dataDirectory = config.get( GraphDatabaseSettings.data_directory );
        var databasesRootDirectory = config.get( GraphDatabaseInternalSettings.databases_root_path );
        var txLogsRootDirectory = config.get( GraphDatabaseSettings.transaction_logs_root_path );
        var scriptRootDirectory = config.get( GraphDatabaseSettings.script_root_path );
        return new Neo4jLayout( homeDirectory, dataDirectory, databasesRootDirectory, txLogsRootDirectory, scriptRootDirectory );
    }

    public static Neo4jLayout ofFlat( Path homeDirectory )
    {
        var home = homeDirectory.toAbsolutePath();
        var config = Config.newBuilder()
                .set( GraphDatabaseSettings.neo4j_home, home )
                .set( GraphDatabaseSettings.data_directory, home )
                .set( GraphDatabaseSettings.transaction_logs_root_path, home )
                .set( GraphDatabaseInternalSettings.databases_root_path, home )
                .build();
        return of( config );
    }

    private Neo4jLayout( Path homeDirectory, Path dataDirectory, Path databasesRootDirectory, Path txLogsRootDirectory, Path scriptRootDirectory )
    {
        this.homeDirectory = FileUtils.getCanonicalFile( homeDirectory );
        this.dataDirectory = FileUtils.getCanonicalFile( dataDirectory );
        this.databasesRootDirectory = FileUtils.getCanonicalFile( databasesRootDirectory );
        this.txLogsRootDirectory = FileUtils.getCanonicalFile( txLogsRootDirectory );
        this.scriptRootDirectory = FileUtils.getCanonicalFile( scriptRootDirectory );
    }

    /**
     * Try to return database layouts for directories that located in the current store directory.
     * Each sub directory of the store directory treated as a separate database directory and database layout wrapper build for that.
     *
     * @return database layouts for directories located in current store directory. If no subdirectories exist empty collection is returned.
     */
    public Collection<DatabaseLayout> databaseLayouts()
    {
        try ( Stream<Path> list = Files.list( databasesRootDirectory) )
        {
            return list.filter( Files::isDirectory ).map( directory -> DatabaseLayout.of( this, directory.getFileName().toString() ) ).collect( toList() );
        }
        catch ( NoSuchFileException e )
        {
            return emptyList();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    public DatabaseLayout databaseLayout( String databaseName )
    {
        return DatabaseLayout.of( this, databaseName );
    }

    public Path databasesDirectory()
    {
        return databasesRootDirectory;
    }

    public Path homeDirectory()
    {
        return homeDirectory;
    }

    public Path transactionLogsRootDirectory()
    {
        return txLogsRootDirectory;
    }

    public Path scriptRootDirectory()
    {
        return scriptRootDirectory;
    }

    public Path dataDirectory()
    {
        return dataDirectory;
    }

    public Path storeLockFile()
    {
        return databasesRootDirectory.resolve( STORE_LOCK_FILENAME );
    }

    public Path serverIdFile()
    {
        return dataDirectory.resolve( SERVER_ID_FILENAME );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        var that = (Neo4jLayout) o;
        return Objects.equals( homeDirectory, that.homeDirectory ) && Objects.equals( dataDirectory, that.dataDirectory ) &&
               Objects.equals( databasesRootDirectory, that.databasesRootDirectory ) && Objects.equals( txLogsRootDirectory, that.txLogsRootDirectory );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( homeDirectory, dataDirectory, databasesRootDirectory, txLogsRootDirectory );
    }

    @Override
    public String toString()
    {
        return String.format( "Neo4JLayout{ homeDir=%s, dataDir=%s, databasesDir=%s, txLogsRootDir=%s}",
                homeDirectory.toString(), dataDirectory.toString(), databasesRootDirectory.toString(), txLogsRootDirectory.toString() );
    }
}
