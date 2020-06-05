/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import React, {Component} from 'react';
import List from '@material-ui/core/List';
import ListItem from '@material-ui/core/ListItem';
import ListItemText from '@material-ui/core/ListItemText';
import TypoGraphy from '@material-ui/core/Typography'
import {
    withRouter,
    NavLink 
    
} from 'react-router-dom'


class NavBar extends Component {

    render() {
        return (
            <List component="nav">
                <ListItem component="div" >
                    <ListItemText inset onClick={() => this.props.history.push('/dashboard/cluster')}>
                        <TypoGraphy color="inherit" variant="title" >
                        <NavLink 
                        to="/dashboard/cluster"
                        activeStyle={{
                            
                            fontWeight: "bold",
                            
                            color: "white"
                        }}>
                            Cluster
                            </NavLink>  
                        </TypoGraphy>
                    </ListItemText>

                    <ListItemText inset onClick={() => this.props.history.push('/dashboard/tenants')}>
                        <TypoGraphy color="inherit" variant="title">
                        <NavLink
                        to="/dashboard/tenants"
                        activeStyle={{
                            
                            fontWeight: "bold",
                            
                            color: "white"
                        }}>
                            Tenants
                            </NavLink>
                            
                        </TypoGraphy>
                    </ListItemText>

                    <ListItemText inset onClick={() => this.props.history.push('/dashboard/tables')}>
                        <TypoGraphy color="inherit" variant="title">
                        <NavLink
                        to="/dashboard/tables"
                        activeStyle={{
                            
                            fontWeight: "bold",
                            
                            color: "white"
                        }}>
                            Tables
                            </NavLink>
                            
                        </TypoGraphy>
                    </ListItemText>

                    <ListItemText inset onClick={() => this.props.history.push('/dashboard/controllers')}>
                        <TypoGraphy color="inherit" variant="title">
                        <NavLink
                        to="/dashboard/controllers"
                        activeStyle={{
                            
                            fontWeight: "bold",
                            
                            color: "white"
                        }}>
                            Controllers
                            </NavLink>
                        </TypoGraphy>
                    </ListItemText>

                    <ListItemText inset onClick={() => this.props.history.push('/dashboard/servers')}>
                        <TypoGraphy color="inherit" variant="title">
                        <NavLink
                        to="/dashboard/servers"
                        activeStyle={{
                            
                            fontWeight: "bold",
                            
                            color: "white"
                        }}>
                             Servers
                            </NavLink>
                           
                        </TypoGraphy>
                    </ListItemText>
                    {/* onClick={() => this.props.history.push('/brokers')} */}
                    <ListItemText inset  onClick={() => this.props.history.push('/dashboard/brokers')} >
                        <TypoGraphy color="inherit" variant="title">
                        <NavLink
                        to="/dashboard/brokers"
                        activeStyle={{
                            
                            fontWeight: "bold",
                            
                            color: "white"
                        }}
                        >
                            Brokers
                            </NavLink>
                        </TypoGraphy>
                    </ListItemText>
                </ListItem>
            </List>)
    }
}


export default withRouter(NavBar);