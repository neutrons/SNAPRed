def mantidAxisFactory(ax):
    """
    Refurbish the axis object with a new rename_workspace method
    """

    def rename_workspace(old_name, new_name):
        """
        Rename a workspace, and update the artists, creation arguments and tracked workspaces accordingly
        :param new_name : the new name of workspace
        :param old_name : the old name of workspace
        """
        for cargs in ax.creation_args:
            # NEW CHECK
            func_name = cargs["function"]
            if func_name not in ["axhline", "axvline"] and cargs["workspaces"] == old_name:
                cargs["workspaces"] = new_name
            # Alternatively,
            # if cargs.get("workspaces") == old_name:
            #     cargs["workspaces"] = new_name
        for ws_name, ws_artist_list in list(ax.tracked_workspaces.items()):
            for ws_artist in ws_artist_list:
                if ws_artist.workspace_name == old_name:
                    ws_artist.rename_data(new_name)
            if ws_name == old_name:
                ax.tracked_workspaces[new_name] = ax.tracked_workspaces.pop(old_name)

    ax.rename_workspace = rename_workspace
    return ax
