function onOpen() {
    var ui = SpreadsheetApp.getUi();
    // Or DocumentApp or FormApp.
    ui.createMenu('Shopify')
        .addItem('Create Shopify Export', 'shopifyExport')
        // .addSeparator()
        // .addSubMenu(ui.createMenu('Sub-menu')
        //     .addItem('Second item', 'menuItem2'))
        .addToUi();
}

// update cell with new value
function updateCell(cell, value) {
    var sheet = SpreadsheetApp.getActive().getSheetByName('Control');
    sheet.getRange(cell).setValue(value);
}


function shopifyExport() {
    SpreadsheetApp.getUi() // Or DocumentApp or FormApp.
        .alert('Running Export Process...');
    updateCell('B2', 'true')

    sheet = SpreadsheetApp.getActive().getSheetByName('Control')
    while (sheet.getRange(cell).getValue() == 'true') {
        // wait for cell to be updated by Python function.
        Utilities.sleep(5);
        updateCell('B2', 'false')
        if (sheet.getRange('B2').getValue() == 'false') {
            break;
        }

    }

}

// get cell value from sheet
function getCell(cell) {
    var sheet = SpreadsheetApp.getActive().getSheetByName('Control');
    return sheet.getRange(cell).getValue();
}