##################
# Import Modules #

import re
import argparse
from colored import fg, attr

import ummap_oltt_tobii_fixation_summary_helpers as hlps


def main():

    #####################
    # Print Color Setup #

    clr_blu = fg('blue')
    clr_bld = attr('bold')
    clr_wrn = fg('red') + attr('bold')
    clr_rst = attr('reset')

    ##############
    # Parse Args #

    parser = argparse.ArgumentParser(description="Process grouped summary statistics of OLTT Set 11A data.")
    parser.add_argument('-j', '--jwt_cfg', required=True,
                        help=f"{clr_bld}required{clr_rst}: absolute path to JWT config file")
    parser.add_argument('-b', '--box_folder_id', required=True,
                        help=f"{clr_bld}required{clr_rst}: destination Box Folder ID")
    parser.add_argument('-v', '--verbose', action='store_true',
                        help=f"print actions to stdout")
    args = parser.parse_args()

    #################
    # Configuration #

    # Access args.verbose once
    is_verbose = args.verbose

    # Set the path to your JWT app config JSON file
    jwt_cfg_path = args.jwt_cfg
    if is_verbose:
        print(f"{clr_blu}Path to Box JWT config{clr_rst}:", f"{jwt_cfg_path}")

    # Set the path to the folder that will be traversed
    box_folder_id = args.box_folder_id

    ############################
    # Establish Box Connection #

    # Get authenticated Box client
    client = hlps.get_authenticated_client(jwt_cfg_path)

    # Create Box Folder object with authenticated client
    folder = client.folder(folder_id=box_folder_id).get()

    ##########################################
    # Define Regex Patterns for Target Files #

    ptrn_fix = re.compile(r'^.*FixOnly.*\.xlsx$')
    ptrn_gaz = re.compile(r'^.*AllGaze.*\.xlsx$')
    ptrn_fix_summ = re.compile(r'^.*_fixation_summary.xlsx$')

    ##################################################
    # Recurse Through Directories to Summarize Stats #

    # Process all "Fix Only" and "All Gaze" files
    hlps.walk_dir_tree_process_fix_gaz(client, folder, ptrn_fix, ptrn_gaz, ptrn_fix_summ)


if __name__ == "__main__":
    main()
